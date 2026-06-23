package com.example.saga.orchestration;

import com.example.saga.entity.*;
import com.example.saga.model.*;
import com.example.saga.model.SagaStep.StepStatus;
import com.example.saga.order.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * ORCHESTRATION SAGA — Central Orchestrator
 *
 * The Orchestrator is the "brain" of the saga.
 * It calls each service in sequence and decides what to do next.
 *
 * Advantages over Choreography:
 *  ✓ Single place to see the full saga flow
 *  ✓ Easier to debug — one class to look at
 *  ✓ Easier to add/remove steps
 *  ✓ No risk of cyclic event dependencies
 *
 * Disadvantage:
 *  ✗ Creates coupling — orchestrator knows all services
 *  ✗ Single point of failure (mitigate with replicas + DB-backed saga log)
 *
 * Saga flow:
 *  1. processPayment()      → success → step 2 | failure → compensate (nothing to undo yet)
 *  2. reserveInventory()    → success → step 3 | failure → compensate (refund payment)
 *  3. sendNotification()    → success → COMPLETED | failure → log (non-critical)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final OrderService orderService;
    private final SagaStepRepository sagaStepRepository;
    private final OrchestrationPaymentService paymentService;
    private final OrchestrationInventoryService inventoryService;
    private final OrchestrationNotificationService notificationService;

    /**
     * Main entry point for the Orchestration Saga.
     * Called by OrderController when creating an order with ORCHESTRATION saga type.
     */
    @Transactional
    public Order executeSaga(Order order) {
        log.info("[ORCHESTRATOR] Starting saga for order: {}", order.getId());

        // STEP 1: Process Payment
        SagaStep paymentStep = startStep(order.getId(), "PAYMENT");
        try {
            String transactionId = paymentService.processPayment(
                    order.getCustomerId(),
                    order.getTotalAmount()
            );
            completeStep(paymentStep, transactionId);
            orderService.updateOrderStatus(order.getId(), OrderStatus.PAYMENT_APPROVED);
            log.info("[ORCHESTRATOR] Step 1 PAYMENT succeeded. TxId: {}", transactionId);

        } catch (Exception ex) {
            failStep(paymentStep, ex.getMessage());
            log.error("[ORCHESTRATOR] Step 1 PAYMENT failed: {}", ex.getMessage());
            // Nothing to compensate yet — payment failed so no money was taken
            orderService.cancelOrder(order.getId(), "Payment failed: " + ex.getMessage());
            return orderService.findById(order.getId());
        }

        // STEP 2: Reserve Inventory
        SagaStep inventoryStep = startStep(order.getId(), "INVENTORY");
        String transactionId = paymentStep.getResponse();
        try {
            String reservationId = inventoryService.reserveInventory(
                    order.getProductId(),
                    order.getQuantity()
            );
            completeStep(inventoryStep, reservationId);
            orderService.updateOrderStatus(order.getId(), OrderStatus.INVENTORY_RESERVED);
            log.info("[ORCHESTRATOR] Step 2 INVENTORY succeeded. ReservationId: {}", reservationId);

        } catch (Exception ex) {
            failStep(inventoryStep, ex.getMessage());
            log.error("[ORCHESTRATOR] Step 2 INVENTORY failed: {}. Starting compensation.", ex.getMessage());

            // COMPENSATE STEP 1 — Refund payment
            compensatePayment(order.getId(), transactionId);
            orderService.cancelOrder(order.getId(), "Inventory failed: " + ex.getMessage());
            return orderService.findById(order.getId());
        }

        // STEP 3: Send Notification
        SagaStep notificationStep = startStep(order.getId(), "NOTIFICATION");
        try {
            notificationService.sendOrderConfirmation(
                    order.getCustomerId(),
                    order.getId()
            );
            completeStep(notificationStep, "SENT");
            log.info("[ORCHESTRATOR] Step 3 NOTIFICATION succeeded.");

        } catch (Exception ex) {
            // Notification failure is non-critical — don't roll back the order
            // Just log and mark the step as failed but continue
            failStep(notificationStep, ex.getMessage());
            log.warn("[ORCHESTRATOR] Step 3 NOTIFICATION failed (non-critical): {}", ex.getMessage());
        }

        // SAGA COMPLETED
        orderService.completeOrder(order.getId());
        log.info("[ORCHESTRATOR] Saga COMPLETED for order: {}", order.getId());
        return orderService.findById(order.getId());
    }

    // ─── Compensation ─────────────────────────────────────────────────────────

    private void compensatePayment(String orderId, String transactionId) {
        SagaStep compensationStep = startStep(orderId, "PAYMENT_COMPENSATION");
        try {
            paymentService.refundPayment(transactionId);
            compensationStep.setStatus(StepStatus.COMPENSATED);
            sagaStepRepository.save(compensationStep);
            log.info("[ORCHESTRATOR] COMPENSATED payment for order: {}", orderId);
        } catch (Exception ex) {
            failStep(compensationStep, "COMPENSATION FAILED: " + ex.getMessage());
            log.error("[ORCHESTRATOR] COMPENSATION FAILED for order: {}. Manual intervention required!", orderId);
            // In production: alert PagerDuty / send to dead-letter queue
        }
    }

    // ─── Saga Step Logging ────────────────────────────────────────────────────

    private SagaStep startStep(String orderId, String stepName) {
        SagaStep step = SagaStep.builder()
                .orderId(orderId)
                .stepName(stepName)
                .status(StepStatus.STARTED)
                .startedAt(LocalDateTime.now())
                .build();
        return sagaStepRepository.save(step);
    }

    private void completeStep(SagaStep step, String response) {
        step.setStatus(StepStatus.COMPLETED);
        step.setResponse(response);
        step.setCompletedAt(LocalDateTime.now());
        sagaStepRepository.save(step);
    }

    private void failStep(SagaStep step, String reason) {
        step.setStatus(StepStatus.FAILED);
        step.setFailureReason(reason);
        step.setCompletedAt(LocalDateTime.now());
        sagaStepRepository.save(step);
    }
}