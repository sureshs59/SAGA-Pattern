package com.example.saga.choreography;


import com.example.saga.model.OrderStatus;
import com.example.saga.model.SagaEvents.*;
import com.example.saga.order.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * CHOREOGRAPHY SAGA — Payment Service
 *
 * Role in the saga:
 *  LISTEN:  order-created topic
 *  DO:      Process payment
 *  PUBLISH: payment-processed (success) OR payment-failed (failure)
 *
 * Compensation:
 *  LISTEN:  inventory-failed topic
 *  DO:      Refund the payment
 *  (No publish needed — OrderService handles cancellation)
 *
 * This service has NO knowledge of other services.
 * It only reacts to events and publishes events.
 * That is the core principle of Choreography.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChoreographyPaymentService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderService orderService;

    @Value("${kafka.topic.payment-processed}") private String paymentProcessedTopic;
    @Value("${kafka.topic.payment-failed}")    private String paymentFailedTopic;

    /**
     * STEP 2 in the saga.
     * Listens to OrderCreatedEvent, processes payment.
     */
    @KafkaListener(
        topics = "${kafka.topic.order-created}",
        groupId = "payment-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("[CHOREOGRAPHY] Payment Service received OrderCreatedEvent for order: {}",
                event.getOrderId());

        try {
            // Simulate payment processing
            boolean paymentSuccess = processPayment(event.getCustomerId(), event.getTotalAmount());

            if (paymentSuccess) {
                // Update order status
                orderService.updateOrderStatus(event.getOrderId(), OrderStatus.PAYMENT_APPROVED);

                // Publish success event — Inventory Service will pick this up
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
                // Payment failed — publish failure event
                // No compensation needed yet (nothing to undo)
                orderService.updateOrderStatus(event.getOrderId(), OrderStatus.PAYMENT_FAILED);

                PaymentFailedEvent failed = PaymentFailedEvent.builder()
                        .orderId(event.getOrderId())
                        .customerId(event.getCustomerId())
                        .reason("Insufficient funds")
                        .timestamp(LocalDateTime.now())
                        .build();

                kafkaTemplate.send(paymentFailedTopic, event.getOrderId(), failed);
                log.warn("[CHOREOGRAPHY] Payment FAILED for order: {}", event.getOrderId());

                // Cancel the order
                orderService.cancelOrder(event.getOrderId(), "Payment failed: Insufficient funds");
            }

        } catch (Exception ex) {
            log.error("[CHOREOGRAPHY] Payment processing error for order: {}", event.getOrderId(), ex);
            orderService.cancelOrder(event.getOrderId(), "Payment error: " + ex.getMessage());
        }
    }

    /**
     * COMPENSATION STEP — Refund payment if inventory reservation fails.
     * This is the compensating transaction for the payment step.
     */
    @KafkaListener(
        topics = "${kafka.topic.inventory-failed}",
        groupId = "payment-compensation-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleInventoryFailed(InventoryFailedEvent event) {
        log.warn("[CHOREOGRAPHY] COMPENSATING — Refunding payment for order: {}",
                event.getOrderId());

        try {
            // Refund the payment (compensating transaction)
            refundPayment(event.getTransactionId());
            log.info("[CHOREOGRAPHY] Payment refunded for order: {}", event.getOrderId());

            // Cancel the order
            orderService.cancelOrder(event.getOrderId(),
                    "Inventory failed: " + event.getReason());

        } catch (Exception ex) {
            log.error("[CHOREOGRAPHY] COMPENSATION FAILED for order: {}", event.getOrderId(), ex);
            // In production: alert on-call, add to dead-letter queue for manual review
        }
    }

    // ─── Simulated Payment Processing ────────────────────────────────────────

    private boolean processPayment(String customerId, java.math.BigDecimal amount) {
        // Simulate payment gateway call
        // In production: call Stripe, Braintree, or your bank's payment API
        log.debug("Processing payment of {} for customer {}", amount, customerId);

        // Simulate 80% success rate for demo
        return !customerId.endsWith("FAIL");
    }

    private void refundPayment(String transactionId) {
        // Simulate refund call to payment gateway
        log.debug("Refunding transaction: {}", transactionId);
        // In production: call payment gateway refund API
    }
}
