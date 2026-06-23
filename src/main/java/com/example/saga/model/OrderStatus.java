package com.example.saga.model;

/**
 * Represents the current status of an Order in the Saga workflow.
 * Each status maps to a step in the distributed transaction.
 */
public enum OrderStatus {
    PENDING,            // Order created, saga not started
    PAYMENT_PENDING,    // Waiting for payment service
    PAYMENT_APPROVED,   // Payment succeeded
    PAYMENT_FAILED,     // Payment failed - compensate
    INVENTORY_PENDING,  // Waiting for inventory service
    INVENTORY_RESERVED, // Inventory reserved
    INVENTORY_FAILED,   // Inventory failed - compensate payment
    NOTIFICATION_SENT,  // Notification sent
    COMPLETED,          // All saga steps succeeded
    CANCELLED           // Saga compensated (rolled back)
}
 