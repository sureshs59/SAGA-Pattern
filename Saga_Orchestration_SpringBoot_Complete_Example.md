# Saga Orchestration in Spring Boot — Complete Real-Time Example
### FedEx Shipment Order flow · event-driven over Kafka · persisted saga state

> **The scenario.** Placing a shipment order spans three microservices — **Order**, **Payment**, **Inventory** — each with its own database. There's no distributed ACID transaction, so we coordinate with a **Saga**. An **orchestrator** drives the steps and, on any failure, runs **compensating transactions in reverse** to leave the system consistent. The orchestrator persists saga state after every step so it survives a crash mid-saga.

---

## 1. Architecture & happy/failure flow

```
                         +-------------------------------+
                         |   Shipment Saga Orchestrator  |
                         |   (owns sequence + state)     |
   POST /shipments ----> |   saga state persisted in DB  |
                         +-------------------------------+
                            |  commands (Kafka)   ^  reply events (Kafka)
            +---------------+----------+----------+----------------+
            v                          v                           v
   +----------------+        +------------------+        +-------------------+
   | Order Service  |        | Payment Service  |        | Inventory Service |
   | order-db       |        | payment-db       |        | inventory-db      |
   | create/cancel  |        | charge/refund    |        | reserve/release   |
   +----------------+        +------------------+        +-------------------+

   HAPPY PATH (each step = a local transaction in that service's own DB):
     1. CreateOrder    -> OrderCreated
     2. ChargePayment  -> PaymentCharged
     3. ReserveStock   -> StockReserved
     -> saga COMPLETED

   FAILURE (e.g. step 3 ReserveStock fails - out of stock):
     orchestrator runs COMPENSATIONS in reverse:
     2c. RefundPayment -> PaymentRefunded
     1c. CancelOrder   -> OrderCancelled
     -> saga COMPENSATED  (system back to consistent state)
```

**State machine the orchestrator persists:**
```
   STARTED
      | CreateOrder ok
      v
   ORDER_CREATED
      | ChargePayment ok
      v
   PAYMENT_CHARGED
      | ReserveStock ok ----------------> COMPLETED   (success terminal)
      | ReserveStock FAIL
      v
   COMPENSATING
      | RefundPayment ok, CancelOrder ok
      v
   COMPENSATED   (failure terminal)
```

---

## 2. Project layout

```
saga-shipment/
  orchestrator-service/        # the Saga coordinator
    SagaInstance.java          # persisted saga state (JPA entity)
    SagaStep.java / SagaState  # enums
    ShipmentSagaOrchestrator.java   # the core logic
    SagaCommandPublisher.java  # sends commands to Kafka
    SagaReplyListener.java     # listens for reply events
    ShipmentController.java    # REST entry point
  order-service/
    OrderCommandListener.java  # handles CreateOrder / CancelOrder
  payment-service/
    PaymentCommandListener.java# handles ChargePayment / RefundPayment
  inventory-service/
    InventoryCommandListener.java # handles ReserveStock / ReleaseStock
  common/
    commands & events (shared DTOs / topics)
```

---

## 3. Shared contracts (commands, events, topics)

```java
// ---- common/SagaTopics.java ----
public final class SagaTopics {
    public static final String ORDER_COMMANDS     = "order.commands";
    public static final String PAYMENT_COMMANDS   = "payment.commands";
    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String SAGA_REPLIES       = "saga.replies";   // all services reply here
    private SagaTopics() {}
}

// ---- common/SagaMessage.java ----
// One envelope for every command and reply, correlated by sagaId.
public record SagaMessage(
        String sagaId,        // correlation id - ties every message to one saga instance
        String type,          // e.g. "CreateOrder", "OrderCreated", "ChargePayment"
        boolean success,      // meaningful on replies
        String payloadJson    // command/event-specific data
) {}
```

The single rule that makes this work: **every message carries the `sagaId`**, so the orchestrator can always look up which saga a reply belongs to.

---

## 4. The persisted saga state (this is what survives a crash)

```java
// ---- orchestrator-service/SagaState.java ----
public enum SagaState {
    STARTED, ORDER_CREATED, PAYMENT_CHARGED, COMPLETED,   // forward path
    COMPENSATING, COMPENSATED, FAILED                     // failure path
}

// ---- orchestrator-service/SagaInstance.java ----
@Entity
@Table(name = "saga_instance")
public class SagaInstance {

    @Id
    private String sagaId;                 // UUID, also the Kafka correlation key

    @Enumerated(EnumType.STRING)
    private SagaState state;

    // business data needed to drive steps AND to compensate
    private String customerId;
    private Long   orderId;                // filled after OrderCreated (needed to cancel)
    private String paymentId;              // filled after PaymentCharged (needed to refund)
    private String itemsJson;
    private Long   amountCents;

    private Instant updatedAt;

    // --- getters/setters omitted for brevity ---
    // helper:
    public void advanceTo(SagaState next) {
        this.state = next;
        this.updatedAt = Instant.now();
    }
}

// ---- orchestrator-service/SagaInstanceRepository.java ----
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, String> {}
```

**Why persist?** If the orchestrator pod crashes after `PAYMENT_CHARGED` but before reserving stock, on restart it reads the row, sees state `PAYMENT_CHARGED`, and knows exactly where to resume (or how to compensate). Without persistence, a crash leaves money charged and nothing tracking it.

---

## 5. The orchestrator — the heart of the pattern

```java
// ---- orchestrator-service/ShipmentSagaOrchestrator.java ----
@Service
public class ShipmentSagaOrchestrator {

    private final SagaInstanceRepository repo;
    private final SagaCommandPublisher publisher;

    public ShipmentSagaOrchestrator(SagaInstanceRepository repo, SagaCommandPublisher publisher) {
        this.repo = repo;
        this.publisher = publisher;
    }

    // ===== ENTRY POINT: start a new saga =====
    @Transactional
    public String start(ShipmentRequest req) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId(UUID.randomUUID().toString());
        saga.setCustomerId(req.customerId());
        saga.setItemsJson(req.itemsJson());
        saga.setAmountCents(req.amountCents());
        saga.advanceTo(SagaState.STARTED);
        repo.save(saga);                              // persist BEFORE sending first command

        // Step 1: ask Order service to create the order
        publisher.send(SagaTopics.ORDER_COMMANDS,
                new SagaMessage(saga.getSagaId(), "CreateOrder", true, req.itemsJson()));
        return saga.getSagaId();
    }

    // ===== REPLY HANDLER: driven by SagaReplyListener for every reply event =====
    @Transactional
    public void onReply(SagaMessage reply) {
        SagaInstance saga = repo.findById(reply.sagaId())
                .orElseThrow(() -> new IllegalStateException("Unknown saga " + reply.sagaId()));

        // If we're already compensating, route replies to the compensation handler
        if (saga.getState() == SagaState.COMPENSATING) {
            handleCompensationReply(saga, reply);
            return;
        }

        switch (reply.type()) {

            case "OrderCreated" -> {
                if (reply.success()) {
                    saga.setOrderId(Long.valueOf(reply.payloadJson()));  // remember for compensation
                    saga.advanceTo(SagaState.ORDER_CREATED);
                    repo.save(saga);
                    // Step 2: charge payment
                    publisher.send(SagaTopics.PAYMENT_COMMANDS,
                        new SagaMessage(saga.getSagaId(), "ChargePayment", true,
                                        String.valueOf(saga.getAmountCents())));
                } else {
                    // first step failed - nothing to compensate, just fail
                    saga.advanceTo(SagaState.FAILED);
                    repo.save(saga);
                }
            }

            case "PaymentCharged" -> {
                if (reply.success()) {
                    saga.setPaymentId(reply.payloadJson());              // remember for refund
                    saga.advanceTo(SagaState.PAYMENT_CHARGED);
                    repo.save(saga);
                    // Step 3: reserve stock
                    publisher.send(SagaTopics.INVENTORY_COMMANDS,
                        new SagaMessage(saga.getSagaId(), "ReserveStock", true, saga.getItemsJson()));
                } else {
                    // payment failed AFTER order created -> compensate order
                    beginCompensation(saga);
                }
            }

            case "StockReserved" -> {
                if (reply.success()) {
                    saga.advanceTo(SagaState.COMPLETED);                 // success terminal
                    repo.save(saga);
                } else {
                    // stock failed AFTER order + payment -> compensate both
                    beginCompensation(saga);
                }
            }

            default -> throw new IllegalStateException("Unexpected reply " + reply.type());
        }
    }

    // ===== COMPENSATION: undo committed steps IN REVERSE =====
    private void beginCompensation(SagaInstance saga) {
        saga.advanceTo(SagaState.COMPENSATING);
        repo.save(saga);
        // reverse order: refund payment first (if charged), then cancel order
        if (saga.getPaymentId() != null) {
            publisher.send(SagaTopics.PAYMENT_COMMANDS,
                new SagaMessage(saga.getSagaId(), "RefundPayment", true, saga.getPaymentId()));
        } else if (saga.getOrderId() != null) {
            publisher.send(SagaTopics.ORDER_COMMANDS,
                new SagaMessage(saga.getSagaId(), "CancelOrder", true, String.valueOf(saga.getOrderId())));
        } else {
            saga.advanceTo(SagaState.COMPENSATED);
            repo.save(saga);
        }
    }

    private void handleCompensationReply(SagaInstance saga, SagaMessage reply) {
        switch (reply.type()) {
            case "PaymentRefunded" -> {
                // payment undone; now cancel the order
                if (saga.getOrderId() != null) {
                    publisher.send(SagaTopics.ORDER_COMMANDS,
                        new SagaMessage(saga.getSagaId(), "CancelOrder", true,
                                        String.valueOf(saga.getOrderId())));
                } else {
                    saga.advanceTo(SagaState.COMPENSATED);
                    repo.save(saga);
                }
            }
            case "OrderCancelled" -> {
                saga.advanceTo(SagaState.COMPENSATED);                   // failure terminal
                repo.save(saga);
            }
            // NOTE: if a compensation itself fails, retry (idempotent) or flag for
            // manual intervention + alert. Compensations must never be silently dropped.
            default -> throw new IllegalStateException("Unexpected compensation reply " + reply.type());
        }
    }
}
```

---

## 6. Kafka wiring (publisher + reply listener)

```java
// ---- orchestrator-service/SagaCommandPublisher.java ----
@Component
public class SagaCommandPublisher {
    private final KafkaTemplate<String, SagaMessage> kafka;

    public SagaCommandPublisher(KafkaTemplate<String, SagaMessage> kafka) { this.kafka = kafka; }

    public void send(String topic, SagaMessage msg) {
        // key by sagaId so all messages for one saga land on the same partition (ordering)
        kafka.send(topic, msg.sagaId(), msg);
    }
}

// ---- orchestrator-service/SagaReplyListener.java ----
@Component
public class SagaReplyListener {
    private final ShipmentSagaOrchestrator orchestrator;

    public SagaReplyListener(ShipmentSagaOrchestrator orchestrator) { this.orchestrator = orchestrator; }

    @KafkaListener(topics = SagaTopics.SAGA_REPLIES, groupId = "orchestrator")
    public void onReply(SagaMessage reply) {
        orchestrator.onReply(reply);   // single entry point for every reply event
    }
}

// ---- orchestrator-service/ShipmentController.java ----
@RestController
@RequestMapping("/shipments")
public class ShipmentController {
    private final ShipmentSagaOrchestrator orchestrator;

    public ShipmentController(ShipmentSagaOrchestrator orchestrator) { this.orchestrator = orchestrator; }

    @PostMapping
    public ResponseEntity<Map<String, String>> create(@RequestBody ShipmentRequest req) {
        String sagaId = orchestrator.start(req);
        // 202 Accepted - saga runs asynchronously; client polls or gets notified later
        return ResponseEntity.accepted().body(Map.of("sagaId", sagaId, "status", "PROCESSING"));
    }
}
```

---

## 7. A participant service (Order) — the pattern repeats for Payment & Inventory

```java
// ---- order-service/OrderCommandListener.java ----
@Component
public class OrderCommandListener {

    private final OrderRepository orders;
    private final KafkaTemplate<String, SagaMessage> kafka;

    public OrderCommandListener(OrderRepository orders, KafkaTemplate<String, SagaMessage> kafka) {
        this.orders = orders;
        this.kafka = kafka;
    }

    @KafkaListener(topics = SagaTopics.ORDER_COMMANDS, groupId = "order-service")
    @Transactional                                  // LOCAL transaction in order-db
    public void onCommand(SagaMessage cmd) {
        switch (cmd.type()) {

            case "CreateOrder" -> {
                try {
                    Order order = new Order();
                    order.setItemsJson(cmd.payloadJson());
                    order.setStatus("CREATED");
                    orders.save(order);             // local commit
                    reply(cmd.sagaId(), "OrderCreated", true, String.valueOf(order.getId()));
                } catch (Exception e) {
                    reply(cmd.sagaId(), "OrderCreated", false, e.getMessage());
                }
            }

            case "CancelOrder" -> {                 // COMPENSATION - must be idempotent
                Long orderId = Long.valueOf(cmd.payloadJson());
                orders.findById(orderId).ifPresent(o -> {
                    if (!"CANCELLED".equals(o.getStatus())) {   // idempotency guard
                        o.setStatus("CANCELLED");
                        orders.save(o);
                    }
                });
                reply(cmd.sagaId(), "OrderCancelled", true, String.valueOf(orderId));
            }

            default -> { /* ignore commands not for this service */ }
        }
    }

    private void reply(String sagaId, String type, boolean ok, String payload) {
        kafka.send(SagaTopics.SAGA_REPLIES, sagaId, new SagaMessage(sagaId, type, ok, payload));
    }
}
```

Payment (`ChargePayment`/`RefundPayment`) and Inventory (`ReserveStock`/`ReleaseStock`) follow the **exact same shape**: a `@KafkaListener` + `@Transactional` local DB write + a reply event. The compensations (`RefundPayment`, `ReleaseStock`, `CancelOrder`) all carry an **idempotency guard** so a retried compensation can't double-refund or double-cancel.

---

## 8. The three production-grade details that separate senior answers

1. **Idempotency.** Every command handler — especially compensations — must be safe to run twice, because Kafka gives at-least-once delivery. Guard with a status check (as in `CancelOrder` above) or a processed-message table.

2. **Ordering & correlation.** Keying Kafka messages by `sagaId` keeps all of one saga's messages on the same partition, preserving order, and lets the orchestrator correlate replies to the persisted saga row.

3. **Crash recovery.** Because saga state is persisted after every step, a crashed orchestrator resumes from the last known state on restart. A scheduled sweep can also find sagas stuck in a non-terminal state past a timeout and drive them to compensation.

---

## 9. application.yml (orchestrator)

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: orchestrator
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
  datasource:
    url: jdbc:postgresql://localhost:5432/saga_db
    username: saga
    password: saga
  jpa:
    hibernate.ddl-auto: update
```

---

## 10. How to explain this in 60 seconds (interview compression)

> "For the FedEx shipment flow, placing an order spanned Order, Payment, and Inventory — separate databases, no distributed transaction. I used a Saga with an orchestrator. The orchestrator persists saga state and drives the steps over Kafka: create order, charge payment, reserve stock — each a local transaction with a reply event. If a step fails, say stock is out, the orchestrator runs compensations in reverse: refund the payment, cancel the order. The three things I'd call out as production-critical are idempotent handlers because Kafka is at-least-once, keying messages by saga ID for ordering and correlation, and persisting saga state so a crash mid-flow can resume or compensate rather than leaking a charge."

---

## Honest caveats

- This is a **hand-rolled orchestrator** to show the mechanics clearly. In production you'd often use a framework that manages the persisted state machine for you — **Axon Framework**, **Eventuate Tram Saga**, or a workflow engine like **Temporal** / **Camunda**. Know the names; verify current versions and APIs before claiming hands-on depth, since that ecosystem moves.
- The code targets recent **Spring Boot 3 / Spring Kafka** (records, `KafkaTemplate`, lambda config). On Spring Boot 2.x some APIs differ slightly. (Note: Spring Boot 3 requires Java 17+, so if your résumé says Java 11 for a given project, pair that project with Spring Boot 2.x to stay consistent.)
- The `@Transactional` on a Kafka listener commits the **DB** transaction; it does not make the DB write and the Kafka reply atomic. True atomicity there needs the **transactional outbox pattern** (write the event to an outbox table in the same DB transaction, relay it to Kafka separately) — worth mentioning as the next level if they push on "what if the DB commits but the reply never sends?"

*End of example.*
