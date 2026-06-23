package com.example.saga.choreography;


import com.example.saga.model.SagaEvents.*;
import com.example.saga.order.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * CHOREOGRAPHY SAGA — Notification Service (Final Step)
 *
 * Role in the saga:
 *  LISTEN: inventory-reserved (happy path) OR order-cancelled (compensation)
 *  DO:     Send notification to customer
 *  PUBLISH: Nothing — this is the last step
 *
 * This service completes or closes the saga from the customer perspective.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChoreographyNotificationService {

    private final OrderService orderService;

    /**
     * STEP 4 (Final) — Happy path.
     * Inventory reserved successfully. Send confirmation email/SMS.
     */
    @KafkaListener(
        topics = "${kafka.topic.inventory-reserved}",
        groupId = "notification-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleInventoryReserved(InventoryReservedEvent event) {
        log.info("[CHOREOGRAPHY] Notification Service — sending ORDER CONFIRMATION for order: {}",
                event.getOrderId());

        sendNotification(
            orderService.findById(event.getOrderId()).getCustomerId(),
            "ORDER_CONFIRMED",
            "Your order " + event.getOrderId() + " has been confirmed! " +
            "Reserved " + event.getQuantity() + " item(s)."
        );

        // Mark order as completed
        orderService.completeOrder(event.getOrderId());
        log.info("[CHOREOGRAPHY] Saga COMPLETED for order: {}", event.getOrderId());
    }

    /**
     * COMPENSATION notification — order was cancelled.
     * Notify the customer that their order failed.
     */
    @KafkaListener(
        topics = "${kafka.topic.order-cancelled}",
        groupId = "notification-cancellation-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderCancelled(OrderCancelledEvent event) {
        log.warn("[CHOREOGRAPHY] Notification Service — sending CANCELLATION for order: {}",
                event.getOrderId());

        sendNotification(
            event.getCustomerId(),
            "ORDER_CANCELLED",
            "Sorry, your order " + event.getOrderId() + " was cancelled. " +
            "Reason: " + event.getReason() + ". Any payment has been refunded."
        );
    }

    // ─── Simulated Notification Sending ──────────────────────────────────────

    private void sendNotification(String customerId, String type, String message) {
        // In production: integrate with SendGrid (email), Twilio (SMS),
        // Firebase Cloud Messaging (push notification), etc.
        log.info("[NOTIFICATION] To: {} | Type: {} | Message: {}", customerId, type, message);
    }
}
