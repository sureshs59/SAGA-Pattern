package com.example.saga.choreography;


import com.example.saga.model.OrderStatus;
import com.example.saga.model.SagaEvents.InventoryFailedEvent;
import com.example.saga.model.SagaEvents.InventoryReservedEvent;
import com.example.saga.model.SagaEvents.PaymentProcessedEvent;
import com.example.saga.model.*;
import com.example.saga.order.OrderService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CHOREOGRAPHY SAGA — Inventory Service
 *
 * Role in the saga:
 *  LISTEN:  payment-processed topic
 *  DO:      Reserve inventory
 *  PUBLISH: inventory-reserved (success) OR inventory-failed (failure)
 *
 * Compensation:
 *  If this step fails, it publishes inventory-failed which triggers
 *  the Payment Service to refund.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChoreographyInventoryService {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final OrderService orderService;

    @Value("${kafka.topic.inventory-reserved}") private String inventoryReservedTopic;
    @Value("${kafka.topic.inventory-failed}")   private String inventoryFailedTopic;

    // Simulated inventory store (use real DB in production)
    private final Map<String, Integer> inventoryStore = new ConcurrentHashMap<>(Map.of(
            "PRODUCT_001", 100,
            "PRODUCT_002", 0,     // out of stock — will cause saga compensation
            "PRODUCT_003", 50
    ));

//    public ChoreographyInventoryService(KafkaTemplate<Object, Object>  kafkaTemplate, OrderService orderService) {
//		this.kafkaTemplate = kafkaTemplate;
//		this.orderService = orderService;
//		// Initialize inventory store if needed
//	}
    /**
     * STEP 3 in the saga.
     * Listens to PaymentProcessedEvent, reserves inventory.
     */
    @KafkaListener(
        topics = "${kafka.topic.payment-processed}",
        groupId = "inventory-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handlePaymentProcessed(PaymentProcessedEvent event) {
        log.info("[CHOREOGRAPHY] Inventory Service received PaymentProcessedEvent for order: {}",
                event.getOrderId());

        orderService.updateOrderStatus(event.getOrderId(), OrderStatus.INVENTORY_PENDING);

        // Get order details to know which product/quantity
        var order = orderService.findById(event.getOrderId());

        try {
            boolean reserved = reserveInventory(order.getProductId(), order.getQuantity());

            if (reserved) {
                orderService.updateOrderStatus(event.getOrderId(), OrderStatus.INVENTORY_RESERVED);

                InventoryReservedEvent reservedEvent = InventoryReservedEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(order.getProductId())
                        .quantity(order.getQuantity())
                        .reservationId(UUID.randomUUID().toString())
                        .timestamp(LocalDateTime.now())
                        .build();

                kafkaTemplate.send(inventoryReservedTopic, event.getOrderId(), reservedEvent);
                log.info("[CHOREOGRAPHY] Inventory RESERVED for order: {}", event.getOrderId());

            } else {
                // Inventory failed — trigger compensation (payment refund)
                orderService.updateOrderStatus(event.getOrderId(), OrderStatus.INVENTORY_FAILED);

                InventoryFailedEvent failedEvent = InventoryFailedEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(order.getProductId())
                        .reason("Insufficient inventory for product: " + order.getProductId())
                        .transactionId(event.getTransactionId()) // needed for refund
                        .timestamp(LocalDateTime.now())
                        .build();

                kafkaTemplate.send(inventoryFailedTopic, event.getOrderId(), failedEvent);
                log.warn("[CHOREOGRAPHY] Inventory FAILED for order: {} — triggering compensation",
                        event.getOrderId());
            }

        } catch (Exception ex) {
            log.error("[CHOREOGRAPHY] Inventory error for order: {}", event.getOrderId(), ex);

            InventoryFailedEvent failedEvent = InventoryFailedEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(order.getProductId())
                    .reason("Inventory error: " + ex.getMessage())
                    .transactionId(event.getTransactionId())
                    .timestamp(LocalDateTime.now())
                    .build();

            kafkaTemplate.send(inventoryFailedTopic, event.getOrderId(), failedEvent);
        }
    }

    // ─── Simulated Inventory Management ──────────────────────────────────────

    private synchronized boolean reserveInventory(String productId, int quantity) {
        int available = inventoryStore.getOrDefault(productId, 0);
        if (available >= quantity) {
            inventoryStore.put(productId, available - quantity);
            log.debug("Reserved {} units of {}. Remaining: {}", quantity, productId, available - quantity);
            return true;
        }
        log.debug("Insufficient inventory for {}. Available: {}, Required: {}", productId, available, quantity);
        return false;
    }

    public synchronized void releaseInventory(String productId, int quantity) {
        // Compensating transaction — releases reserved inventory
        inventoryStore.merge(productId, quantity, Integer::sum);
        log.debug("Released {} units of {}.", quantity, productId);
    }
}
