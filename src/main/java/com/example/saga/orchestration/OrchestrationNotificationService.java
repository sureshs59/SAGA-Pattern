package com.example.saga.orchestration;


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * ORCHESTRATION SAGA — Notification Service (Final Step)
 *
 * Non-critical step — if this fails the order is still valid.
 * The orchestrator logs the failure but does not compensate.
 */
@Slf4j
@Service
public class OrchestrationNotificationService {

    public void sendOrderConfirmation(String customerId, String orderId) {
        log.info("[ORCHESTRATION] Sending order confirmation to customer: {} for order: {}",
                customerId, orderId);

        // In production: integrate with SendGrid, Twilio, Firebase, SES, etc.
        log.info("[ORCHESTRATION] Email sent: Order {} confirmed for customer {}", orderId, customerId);
    }

    public void sendOrderCancellation(String customerId, String orderId, String reason) {
        log.info("[ORCHESTRATION] Sending cancellation notice to customer: {} for order: {}",
                customerId, orderId);
        log.info("[ORCHESTRATION] Email sent: Order {} cancelled. Reason: {}", orderId, reason);
    }
}
