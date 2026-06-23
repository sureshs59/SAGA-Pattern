package com.example.saga.orchestration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * ORCHESTRATION SAGA — Payment Service
 *
 * In orchestration, services are called DIRECTLY by the orchestrator.
 * No event publishing — the orchestrator calls this like a regular method call
 * (or a synchronous REST/gRPC call in a real microservice setup).
 *
 * This service is simpler than the choreography version because:
 *  - It doesn't need to listen to Kafka topics
 *  - It doesn't need to know what happens next
 *  - It just processes the payment and returns success/failure
 */
@Slf4j
@Service
public class OrchestrationPaymentService {

    /**
     * Process payment and return transaction ID.
     * Throws exception on failure — orchestrator handles compensation.
     */
    public String processPayment(String customerId, BigDecimal amount) {
        log.info("[ORCHESTRATION] Processing payment of {} for customer: {}", amount, customerId);

        // Simulate payment failure for specific customers
        if (customerId.endsWith("FAIL")) {
            throw new RuntimeException("Insufficient funds for customer: " + customerId);
        }

        // Simulate payment gateway latency
        simulateProcessingDelay(100);

        String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("[ORCHESTRATION] Payment SUCCESS. TransactionId: {}", transactionId);
        return transactionId;
    }

    /**
     * Compensating transaction — refund payment.
     * Called by orchestrator if a downstream step fails.
     */
    public void refundPayment(String transactionId) {
        log.info("[ORCHESTRATION] COMPENSATING — Refunding transaction: {}", transactionId);

        // In production: call payment gateway refund API
        simulateProcessingDelay(50);

        log.info("[ORCHESTRATION] Refund SUCCESS for transaction: {}", transactionId);
    }

    private void simulateProcessingDelay(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException ignored) {}
    }
}