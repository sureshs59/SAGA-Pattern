package com.example.saga.model;

import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * All Kafka event DTOs for the Choreography Saga.
 *
 * In Choreography Saga each service:
 *  1. Listens to events from the previous step
 *  2. Does its work
 *  3. Publishes an event for the next step (or a failure event for compensation)
 *
 * Event flow (happy path):
 *  OrderCreatedEvent → PaymentProcessedEvent → InventoryReservedEvent → OrderCompletedEvent
 *
 * Compensation flow:
 *  InventoryFailedEvent → PaymentRefundEvent → OrderCancelledEvent
 */
public class SagaEvents {

    // ─── STEP 1: Order Service publishes ─────────────────────────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class OrderCreatedEvent {
        private String orderId;
        private String customerId;
        private String productId;
        private Integer quantity;
        private BigDecimal totalAmount;
        private LocalDateTime timestamp;
    }

    // ─── STEP 2: Payment Service publishes on success ─────────────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PaymentProcessedEvent {
        private String orderId;
        private String customerId;
        private String transactionId;
        private BigDecimal amount;
        private LocalDateTime timestamp;
    }

    // ─── STEP 2: Payment Service publishes on failure ─────────────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PaymentFailedEvent {
        private String orderId;
        private String customerId;
        private String reason;
        private LocalDateTime timestamp;
    }

    // ─── STEP 3: Inventory Service publishes on success ───────────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class InventoryReservedEvent {
        private String orderId;
        private String productId;
        private Integer quantity;
        private String reservationId;
        private LocalDateTime timestamp;
    }

    // ─── STEP 3: Inventory Service publishes on failure ───────────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class InventoryFailedEvent {
        private String orderId;
        private String productId;
        private String reason;
        private String transactionId; // for payment refund
        private LocalDateTime timestamp;
    }

    // ─── STEP 4: Notification Service publishes ───────────────────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class OrderCompletedEvent {
        private String orderId;
        private String customerId;
        private LocalDateTime timestamp;
    }

    // ─── COMPENSATION: Order Service publishes when saga fails ────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class OrderCancelledEvent {
        private String orderId;
        private String customerId;
        private String reason;
        private LocalDateTime timestamp;
    }

    // ─── NOTIFICATION ──────────────────────────────────────────────────────────
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class NotificationEvent {
        private String orderId;
        private String customerId;
        private String type;    // ORDER_CONFIRMED, ORDER_CANCELLED, PAYMENT_FAILED
        private String message;
        private LocalDateTime timestamp;
    }
}