# Saga Pattern — Spring Boot Complete Guide

> A complete, production-ready implementation of the Saga pattern for distributed transactions
> in a Java Spring Boot banking application using both Choreography and Orchestration approaches.

---

## Table of Contents

1. [What is the Saga Pattern?](#1-what-is-the-saga-pattern)
2. [Choreography vs Orchestration](#2-choreography-vs-orchestration)
3. [Project Structure](#3-project-structure)
4. [Domain Model](#4-domain-model)
5. [Choreography Saga](#5-choreography-saga)
6. [Orchestration Saga](#6-orchestration-saga)
7. [Kafka Configuration](#7-kafka-configuration)
8. [REST API](#8-rest-api)
9. [Testing the Saga](#9-testing-the-saga)
10. [Saga Flow Diagrams](#10-saga-flow-diagrams)
11. [Production Considerations](#11-production-considerations)

---

## 1. What is the Saga Pattern?

### The Problem

In a microservices banking application, a single business operation (like a money transfer
or order placement) spans **multiple services** — Payment, Inventory, Notification, etc.
Each service has its own database. You cannot use a single database transaction (ACID)
across services because they are independent.

**The challenge:** If the payment succeeds but inventory fails — how do you undo the payment?

### The Solution — Saga

A Saga is a sequence of **local transactions**. Each step:
1. Executes a local transaction
2. Publishes a message/event OR calls the next service
3. If a step fails, runs **compensating transactions** to undo previous steps

```
Happy Path:
  CreateOrder → ProcessPayment → ReserveInventory → SendNotification → COMPLETED

Compensation (Inventory fails):
  CreateOrder → ProcessPayment → ReserveInventory(FAIL)
                                  ↓
                           RefundPayment → CancelOrder → CANCELLED
```

### Key Rule

Compensating transactions must be **idempotent** — safe to run multiple times with the same result.
In a network failure, the compensation might be retried.

---

## 2. Choreography vs Orchestration

### Choreography (Event-Driven)

Services communicate through **Kafka events**. Each service:
- Listens to events from the previous step
- Does its work
- Publishes an event for the next step

```
OrderService → [order-created] → PaymentService → [payment-processed] → InventoryService
                                                                              ↓
                                                               [inventory-reserved] → NotificationService
```

**Pros:** Loose coupling, no single point of failure, scales well
**Cons:** Hard to track the overall saga, risk of cyclic events, harder to debug

### Orchestration (Central Controller)

A **SagaOrchestrator** class calls each service directly in sequence.

```
                     ┌─────────────────┐
                     │ SagaOrchestrator │
                     └────────┬────────┘
                              │ calls
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
       PaymentService  InventoryService  NotificationService
```

**Pros:** Easy to understand, single place to debug, easy to add/remove steps
**Cons:** Creates coupling, orchestrator must know all services

### When to Use Which?

| Situation | Use |
|---|---|
| Simple linear workflow | Orchestration |
| Complex branching logic | Orchestration |
| Highly decoupled microservices | Choreography |
| Large teams owning separate services | Choreography |
| Need full audit trail | Orchestration (saga log) |
| Debugging is critical | Orchestration |

---

## 3. Project Structure

```
saga-pattern/
├── pom.xml
└── src/main/java/com/bank/saga/
    ├── SagaPatternApplication.java
    ├── config/
    │   └── KafkaConfig.java                  # Kafka topic configuration
    ├── model/
    │   ├── Order.java                         # Order entity
    │   ├── OrderStatus.java                   # Saga state enum
    │   ├── SagaStep.java                      # Saga log entry (orchestration)
    │   └── SagaEvents.java                    # All Kafka event DTOs
    ├── order/
    │   ├── Order.java
    │   ├── OrderRequest.java
    │   ├── OrderRepository.java
    │   ├── OrderService.java                  # Creates orders, updates status
    │   └── OrderController.java               # REST API
    ├── choreography/
    │   ├── ChoreographyPaymentService.java    # Listens: order-created
    │   ├── ChoreographyInventoryService.java  # Listens: payment-processed
    │   └── ChoreographyNotificationService.java # Listens: inventory-reserved
    └── orchestration/
        ├── SagaOrchestrator.java              # The orchestrator brain
        ├── SagaStepRepository.java
        ├── OrchestrationPaymentService.java
        ├── OrchestrationInventoryService.java
        └── OrchestrationNotificationService.java
```

---

## 4. Domain Model

### OrderStatus Enum

```java
public enum OrderStatus {
    PENDING,            // Order created, saga not started
    PAYMENT_PENDING,    // Waiting for payment service
    PAYMENT_APPROVED,   // Payment succeeded
    PAYMENT_FAILED,     // Payment failed — compensate
    INVENTORY_PENDING,  // Waiting for inventory service
    INVENTORY_RESERVED, // Inventory reserved
    INVENTORY_FAILED,   // Inventory failed — compensate payment
    NOTIFICATION_SENT,  // Notification sent
    COMPLETED,          // All saga steps succeeded
    CANCELLED           // Saga compensated (rolled back)
}
```

### Order Entity

```java
@Entity
@Table(name = "orders")
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class Order {

    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    private String customerId;
    private String productId;
    private Integer quantity;
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private String sagaType;            // CHOREOGRAPHY or ORCHESTRATION
    private boolean paymentCompensated;
    private boolean inventoryCompensated;
    private String failureReason;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
```

### SagaStep Entity (Orchestration Saga Log)

```java
@Entity
@Table(name = "saga_steps")
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class SagaStep {

    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    private String orderId;
    private String stepName;    // PAYMENT, INVENTORY, NOTIFICATION, PAYMENT_COMPENSATION

    @Enumerated(EnumType.STRING)
    private StepStatus status;  // STARTED, COMPLETED, FAILED, COMPENSATING, COMPENSATED

    private String payload;
    private String response;
    private String failureReason;

    private LocalDateTime startedAt;
    private LocalDateTime completedAt;

    public enum StepStatus {
        STARTED, COMPLETED, FAILED, COMPENSATING, COMPENSATED
    }
}
```

### Kafka Event DTOs

```java
public class SagaEvents {

    // Published by: OrderService  |  Consumed by: PaymentService
    public static class OrderCreatedEvent {
        private String orderId, customerId, productId;
        private Integer quantity;
        private BigDecimal totalAmount;
        private LocalDateTime timestamp;
    }

    // Published by: PaymentService (success)  |  Consumed by: InventoryService
    public static class PaymentProcessedEvent {
        private String orderId, customerId, transactionId;
        private BigDecimal amount;
        private LocalDateTime timestamp;
    }

    // Published by: PaymentService (failure)  |  Consumed by: OrderService
    public static class PaymentFailedEvent {
        private String orderId, customerId, reason;
        private LocalDateTime timestamp;
    }

    // Published by: InventoryService (success)  |  Consumed by: NotificationService
    public static class InventoryReservedEvent {
        private String orderId, productId, reservationId;
        private Integer quantity;
        private LocalDateTime timestamp;
    }

    // Published by: InventoryService (failure)  |  Consumed by: PaymentService (COMPENSATION)
    public static class InventoryFailedEvent {
        private String orderId, productId, reason, transactionId;
        private LocalDateTime timestamp;
    }

    // Published by: OrderService  |  Consumed by: NotificationService
    public static class OrderCancelledEvent {
        private String orderId, customerId, reason;
        private LocalDateTime timestamp;
    }
}
```

---

## 5. Choreography Saga

### Payment Service — Step 2

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class ChoreographyPaymentService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderService orderService;

    // STEP 2 — Listen to order-created, process payment
    @KafkaListener(topics = "${kafka.topic.order-created}", groupId = "payment-service-group")
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("[CHOREOGRAPHY] Processing payment for order: {}", event.getOrderId());

        try {
            boolean paymentSuccess = processPayment(event.getCustomerId(), event.getTotalAmount());

            if (paymentSuccess) {
                orderService.updateOrderStatus(event.getOrderId(), OrderStatus.PAYMENT_APPROVED);

                PaymentProcessedEvent processed = PaymentProcessedEvent.builder()
                        .orderId(event.getOrderId())
                        .customerId(event.getCustomerId())
                        .transactionId(UUID.randomUUID().toString())
                        .amount(event.getTotalAmount())
                        .timestamp(LocalDateTime.now())
                        .build();

                kafkaTemplate.send(paymentProcessedTopic, event.getOrderId(), processed);
                log.info("[CHOREOGRAPHY] Payment SUCCESS for order: {}", event.getOrderId());

            } else {
                orderService.cancelOrder(event.getOrderId(), "Payment failed: Insufficient funds");
                // Publish failure event
                kafkaTemplate.send(paymentFailedTopic, event.getOrderId(),
                    PaymentFailedEvent.builder()
                        .orderId(event.getOrderId())
                        .reason("Insufficient funds")
                        .timestamp(LocalDateTime.now()).build());
            }

        } catch (Exception ex) {
            orderService.cancelOrder(event.getOrderId(), "Payment error: " + ex.getMessage());
        }
    }

    // COMPENSATION — Listen to inventory-failed, refund payment
    @KafkaListener(topics = "${kafka.topic.inventory-failed}", groupId = "payment-compensation-group")
    public void handleInventoryFailed(InventoryFailedEvent event) {
        log.warn("[CHOREOGRAPHY] COMPENSATING — Refunding payment for order: {}", event.getOrderId());
        refundPayment(event.getTransactionId());
        orderService.cancelOrder(event.getOrderId(), "Inventory failed: " + event.getReason());
    }
}
```

### Inventory Service — Step 3

```java
@KafkaListener(topics = "${kafka.topic.payment-processed}", groupId = "inventory-service-group")
public void handlePaymentProcessed(PaymentProcessedEvent event) {
    var order = orderService.findById(event.getOrderId());
    boolean reserved = reserveInventory(order.getProductId(), order.getQuantity());

    if (reserved) {
        kafkaTemplate.send(inventoryReservedTopic, event.getOrderId(),
            InventoryReservedEvent.builder()
                .orderId(event.getOrderId())
                .reservationId(UUID.randomUUID().toString())
                .timestamp(LocalDateTime.now()).build());
    } else {
        // Trigger compensation — Payment Service will refund
        kafkaTemplate.send(inventoryFailedTopic, event.getOrderId(),
            InventoryFailedEvent.builder()
                .orderId(event.getOrderId())
                .reason("Insufficient inventory")
                .transactionId(event.getTransactionId())
                .timestamp(LocalDateTime.now()).build());
    }
}
```

### Notification Service — Final Step

```java
// Happy path — order completed
@KafkaListener(topics = "${kafka.topic.inventory-reserved}", groupId = "notification-service-group")
public void handleInventoryReserved(InventoryReservedEvent event) {
    sendNotification(event.getOrderId(), "ORDER_CONFIRMED", "Your order has been confirmed!");
    orderService.completeOrder(event.getOrderId());
}

// Compensation — order cancelled
@KafkaListener(topics = "${kafka.topic.order-cancelled}", groupId = "notification-cancellation-group")
public void handleOrderCancelled(OrderCancelledEvent event) {
    sendNotification(event.getOrderId(), "ORDER_CANCELLED",
        "Your order was cancelled. Reason: " + event.getReason());
}
```

---

## 6. Orchestration Saga

### SagaOrchestrator — Central Controller

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final OrderService orderService;
    private final SagaStepRepository sagaStepRepository;
    private final OrchestrationPaymentService paymentService;
    private final OrchestrationInventoryService inventoryService;
    private final OrchestrationNotificationService notificationService;

    @Transactional
    public Order executeSaga(Order order) {
        log.info("[ORCHESTRATOR] Starting saga for order: {}", order.getId());

        // STEP 1: Process Payment
        SagaStep paymentStep = startStep(order.getId(), "PAYMENT");
        String transactionId;
        try {
            transactionId = paymentService.processPayment(order.getCustomerId(), order.getTotalAmount());
            completeStep(paymentStep, transactionId);
            orderService.updateOrderStatus(order.getId(), OrderStatus.PAYMENT_APPROVED);
        } catch (Exception ex) {
            failStep(paymentStep, ex.getMessage());
            // Nothing to compensate — payment failed, no money taken
            orderService.cancelOrder(order.getId(), "Payment failed: " + ex.getMessage());
            return orderService.findById(order.getId());
        }

        // STEP 2: Reserve Inventory
        SagaStep inventoryStep = startStep(order.getId(), "INVENTORY");
        try {
            String reservationId = inventoryService.reserveInventory(order.getProductId(), order.getQuantity());
            completeStep(inventoryStep, reservationId);
            orderService.updateOrderStatus(order.getId(), OrderStatus.INVENTORY_RESERVED);
        } catch (Exception ex) {
            failStep(inventoryStep, ex.getMessage());
            // COMPENSATE: Refund payment
            compensatePayment(order.getId(), transactionId);
            orderService.cancelOrder(order.getId(), "Inventory failed: " + ex.getMessage());
            return orderService.findById(order.getId());
        }

        // STEP 3: Send Notification (non-critical — don't roll back if this fails)
        SagaStep notificationStep = startStep(order.getId(), "NOTIFICATION");
        try {
            notificationService.sendOrderConfirmation(order.getCustomerId(), order.getId());
            completeStep(notificationStep, "SENT");
        } catch (Exception ex) {
            failStep(notificationStep, ex.getMessage());
            log.warn("[ORCHESTRATOR] Notification failed (non-critical): {}", ex.getMessage());
        }

        orderService.completeOrder(order.getId());
        log.info("[ORCHESTRATOR] Saga COMPLETED for order: {}", order.getId());
        return orderService.findById(order.getId());
    }

    private void compensatePayment(String orderId, String transactionId) {
        SagaStep compensationStep = startStep(orderId, "PAYMENT_COMPENSATION");
        try {
            paymentService.refundPayment(transactionId);
            compensationStep.setStatus(SagaStep.StepStatus.COMPENSATED);
            sagaStepRepository.save(compensationStep);
        } catch (Exception ex) {
            failStep(compensationStep, "COMPENSATION FAILED: " + ex.getMessage());
            // ALERT: Manual intervention required
        }
    }

    private SagaStep startStep(String orderId, String stepName) {
        return sagaStepRepository.save(SagaStep.builder()
                .orderId(orderId).stepName(stepName)
                .status(SagaStep.StepStatus.STARTED)
                .startedAt(LocalDateTime.now()).build());
    }

    private void completeStep(SagaStep step, String response) {
        step.setStatus(SagaStep.StepStatus.COMPLETED);
        step.setResponse(response);
        step.setCompletedAt(LocalDateTime.now());
        sagaStepRepository.save(step);
    }

    private void failStep(SagaStep step, String reason) {
        step.setStatus(SagaStep.StepStatus.FAILED);
        step.setFailureReason(reason);
        step.setCompletedAt(LocalDateTime.now());
        sagaStepRepository.save(step);
    }
}
```

---

## 7. Kafka Configuration

```java
@Configuration
public class KafkaConfig {

    @Bean public NewTopic orderCreatedTopic()     { return build("order-created"); }
    @Bean public NewTopic paymentProcessedTopic() { return build("payment-processed"); }
    @Bean public NewTopic paymentFailedTopic()    { return build("payment-failed"); }
    @Bean public NewTopic inventoryReservedTopic(){ return build("inventory-reserved"); }
    @Bean public NewTopic inventoryFailedTopic()  { return build("inventory-failed"); }
    @Bean public NewTopic orderCompletedTopic()   { return build("order-completed"); }
    @Bean public NewTopic orderCancelledTopic()   { return build("order-cancelled"); }

    private NewTopic build(String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
```

```properties
# application.properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=bank-saga-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.consumer.properties.spring.json.trusted.packages=com.bank.saga.*
```

---

## 8. REST API

### Endpoints

```
POST /api/orders/choreography    Start Choreography Saga (async)
POST /api/orders/orchestration   Start Orchestration Saga (sync)
GET  /api/orders/{orderId}       Get order status
GET  /api/orders/{orderId}/saga-log  Get saga step log
GET  /api/orders                 List all orders
```

### Test Scenarios

```bash
# 1. Happy path (Orchestration)
curl -X POST http://localhost:8080/api/orders/orchestration \
  -H "Content-Type: application/json" \
  -d '{ "customerId": "CUST_001", "productId": "PRODUCT_001", "quantity": 2, "totalAmount": 500.00 }'

# 2. Payment failure (customerId ends with FAIL)
curl -X POST http://localhost:8080/api/orders/orchestration \
  -H "Content-Type: application/json" \
  -d '{ "customerId": "CUST_FAIL", "productId": "PRODUCT_001", "quantity": 2, "totalAmount": 500.00 }'

# 3. Inventory failure (PRODUCT_002 has 0 stock)
curl -X POST http://localhost:8080/api/orders/orchestration \
  -H "Content-Type: application/json" \
  -d '{ "customerId": "CUST_001", "productId": "PRODUCT_002", "quantity": 2, "totalAmount": 500.00 }'

# 4. Check order status
curl http://localhost:8080/api/orders/{orderId}

# 5. Check saga log (orchestration only)
curl http://localhost:8080/api/orders/{orderId}/saga-log
```

### Sample Saga Log Response (Orchestration)

```json
[
  {
    "id": "step-001",
    "orderId": "ORDER-001",
    "stepName": "PAYMENT",
    "status": "COMPLETED",
    "response": "TXN-A1B2C3D4",
    "startedAt": "2026-06-22T10:00:00",
    "completedAt": "2026-06-22T10:00:00.150"
  },
  {
    "id": "step-002",
    "orderId": "ORDER-001",
    "stepName": "INVENTORY",
    "status": "FAILED",
    "failureReason": "Insufficient inventory for PRODUCT_002",
    "startedAt": "2026-06-22T10:00:00.200",
    "completedAt": "2026-06-22T10:00:00.220"
  },
  {
    "id": "step-003",
    "orderId": "ORDER-001",
    "stepName": "PAYMENT_COMPENSATION",
    "status": "COMPENSATED",
    "response": null,
    "startedAt": "2026-06-22T10:00:00.225",
    "completedAt": "2026-06-22T10:00:00.300"
  }
]
```

---

## 9. Testing the Saga

### Unit Test — Orchestration Saga

```java
@ExtendWith(MockitoExtension.class)
class SagaOrchestratorTest {

    @Mock private OrderService orderService;
    @Mock private SagaStepRepository sagaStepRepository;
    @Mock private OrchestrationPaymentService paymentService;
    @Mock private OrchestrationInventoryService inventoryService;
    @Mock private OrchestrationNotificationService notificationService;

    @InjectMocks private SagaOrchestrator orchestrator;

    @Test
    @DisplayName("Happy path: all steps succeed → order COMPLETED")
    void testHappyPath() {
        when(paymentService.processPayment(anyString(), any())).thenReturn("TXN-123");
        when(inventoryService.reserveInventory(anyString(), anyInt())).thenReturn("RES-456");
        when(sagaStepRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Order completedOrder = Order.builder().id("ORDER-001").status(OrderStatus.COMPLETED).build();
        when(orderService.findById("ORDER-001")).thenReturn(completedOrder);

        Order result = orchestrator.executeSaga(testOrder);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.COMPLETED);
        verify(paymentService).processPayment("CUST-001", new BigDecimal("500.00"));
        verify(inventoryService).reserveInventory("PRODUCT-001", 2);
        verify(paymentService, never()).refundPayment(anyString());
    }

    @Test
    @DisplayName("Inventory failure: saga compensates payment → CANCELLED")
    void testInventoryFailure_compensatesPayment() {
        when(paymentService.processPayment(anyString(), any())).thenReturn("TXN-123");
        when(inventoryService.reserveInventory(anyString(), anyInt()))
                .thenThrow(new RuntimeException("Insufficient inventory"));
        when(sagaStepRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Order cancelledOrder = Order.builder().id("ORDER-001").status(OrderStatus.CANCELLED).build();
        when(orderService.findById("ORDER-001")).thenReturn(cancelledOrder);

        Order result = orchestrator.executeSaga(testOrder);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        // Compensation must happen
        verify(paymentService).refundPayment("TXN-123");
        verify(orderService).cancelOrder(eq("ORDER-001"), contains("Inventory failed"));
    }

    @Test
    @DisplayName("Notification failure: saga still COMPLETES (non-critical)")
    void testNotificationFailure_sagaStillCompletes() {
        when(paymentService.processPayment(anyString(), any())).thenReturn("TXN-123");
        when(inventoryService.reserveInventory(anyString(), anyInt())).thenReturn("RES-456");
        doThrow(new RuntimeException("Email server down"))
                .when(notificationService).sendOrderConfirmation(anyString(), anyString());
        when(sagaStepRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Order completedOrder = Order.builder().id("ORDER-001").status(OrderStatus.COMPLETED).build();
        when(orderService.findById("ORDER-001")).thenReturn(completedOrder);

        Order result = orchestrator.executeSaga(testOrder);

        // Still completes despite notification failure
        assertThat(result.getStatus()).isEqualTo(OrderStatus.COMPLETED);
        verify(paymentService, never()).refundPayment(anyString());
        verify(orderService).completeOrder("ORDER-001");
    }
}
```

---

## 10. Saga Flow Diagrams

### Choreography — Happy Path

```
OrderController
     │ POST /choreography
     ▼
OrderService.createOrderChoreography()
     │ publishes → [order-created]
     ▼
ChoreographyPaymentService.handleOrderCreated()
     │ payment SUCCESS
     │ publishes → [payment-processed]
     ▼
ChoreographyInventoryService.handlePaymentProcessed()
     │ inventory RESERVED
     │ publishes → [inventory-reserved]
     ▼
ChoreographyNotificationService.handleInventoryReserved()
     │ sends confirmation email
     │ calls orderService.completeOrder()
     ▼
  ORDER STATUS: COMPLETED ✓
```

### Choreography — Compensation Flow

```
ChoreographyInventoryService.handlePaymentProcessed()
     │ inventory FAILED
     │ publishes → [inventory-failed]
     ▼
ChoreographyPaymentService.handleInventoryFailed()    ← COMPENSATION
     │ refunds payment
     │ calls orderService.cancelOrder()
     │ publishes → [order-cancelled]
     ▼
ChoreographyNotificationService.handleOrderCancelled()
     │ sends cancellation email
     ▼
  ORDER STATUS: CANCELLED ✗
```

### Orchestration — Happy Path

```
OrderController
     │ POST /orchestration
     ▼
SagaOrchestrator.executeSaga()
     │
     ├─ STEP 1: paymentService.processPayment()     → TXN-123   ✓
     │          log step COMPLETED
     │
     ├─ STEP 2: inventoryService.reserveInventory() → RES-456   ✓
     │          log step COMPLETED
     │
     ├─ STEP 3: notificationService.sendConfirmation()          ✓
     │          log step COMPLETED
     │
     └─ orderService.completeOrder()
     ▼
  ORDER STATUS: COMPLETED ✓
```

### Orchestration — Compensation Flow

```
SagaOrchestrator.executeSaga()
     │
     ├─ STEP 1: paymentService.processPayment()     → TXN-123   ✓
     │
     ├─ STEP 2: inventoryService.reserveInventory() → FAILED     ✗
     │          log step FAILED
     │
     │  COMPENSATE:
     ├─ paymentService.refundPayment(TXN-123)                   ↩
     │          log step COMPENSATED
     │
     └─ orderService.cancelOrder()
     ▼
  ORDER STATUS: CANCELLED ✗
```

---

## 11. Production Considerations

### 1. Idempotency

Every compensating transaction must be idempotent — safe to call multiple times.
Use a unique `idempotency-key` header on external API calls:

```java
public void refundPayment(String transactionId) {
    // transactionId is the idempotency key
    // Payment gateway will ignore duplicate refund requests for the same transactionId
    paymentGateway.refund(transactionId);
}
```

### 2. Saga Timeout

Sagas can get stuck if a service is down. Add a timeout mechanism:

```java
@Scheduled(fixedDelay = 60000)
public void checkStuckSagas() {
    LocalDateTime threshold = LocalDateTime.now().minusMinutes(10);
    List<Order> stuckOrders = orderRepository.findByStatusAndCreatedAtBefore(
            OrderStatus.PAYMENT_PENDING, threshold);

    stuckOrders.forEach(order -> {
        log.warn("Saga stuck for order: {}. Cancelling.", order.getId());
        orderService.cancelOrder(order.getId(), "Saga timeout");
    });
}
```

### 3. Dead Letter Queue (DLQ)

Configure Kafka DLQ for messages that fail after all retries:

```properties
spring.kafka.consumer.properties.max.poll.attempts=3
spring.kafka.listener.ack-mode=manual_immediate
```

```java
@KafkaListener(topics = "${kafka.topic.order-created}")
public void handleOrderCreated(OrderCreatedEvent event, Acknowledgment ack) {
    try {
        processPayment(event);
        ack.acknowledge();
    } catch (Exception ex) {
        // After max retries, Spring sends to DLQ automatically
        log.error("Failed to process payment, will be sent to DLQ", ex);
        throw ex;
    }
}
```

### 4. Outbox Pattern (Transactional Outbox)

Avoid dual-write (save to DB + publish to Kafka in same transaction).
Use the Outbox pattern — write to an outbox table in the same DB transaction:

```java
@Transactional
public Order createOrder(OrderRequest request) {
    Order order = orderRepository.save(buildOrder(request));

    // Write event to OUTBOX TABLE (same transaction — atomic)
    outboxRepository.save(OutboxEvent.builder()
            .aggregateId(order.getId())
            .eventType("OrderCreated")
            .payload(toJson(order))
            .build());

    return order;
    // A separate process (Debezium CDC or scheduler) publishes outbox events to Kafka
}
```

### 5. Compensations Must Handle Partial Failures

```java
private void compensatePayment(String orderId, String transactionId) {
    try {
        paymentService.refundPayment(transactionId);
    } catch (Exception ex) {
        log.error("COMPENSATION FAILED for order: {}. Manual intervention required!", orderId);
        // 1. Alert on-call engineer (PagerDuty, OpsGenie)
        // 2. Send to dead-letter queue for manual processing
        // 3. Store in compensation_failures table for audit
        alertService.sendCriticalAlert("Saga compensation failure for order: " + orderId);
    }
}
```

---

## Quick Reference

| Concept | Choreography | Orchestration |
|---|---|---|
| Communication | Kafka events | Direct method calls |
| Coordination | Decentralized | SagaOrchestrator |
| Coupling | Loose | Tight (orchestrator knows all) |
| Debugging | Harder (trace events) | Easier (one class) |
| Saga log | Distributed (Kafka topics) | Centralized (saga_steps table) |
| Adding steps | Update producer + new consumer | Add step in orchestrator |
| Best for | Large decoupled microservices | Linear workflows, banking |

---

*Generated for Java Spring Boot Banking Application — Spring Boot 3.2, Spring Kafka, JPA, JUnit 5*
