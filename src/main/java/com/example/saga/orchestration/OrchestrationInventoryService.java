package com.example.saga.orchestration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ORCHESTRATION SAGA — Inventory Service
 *
 * Called directly by the SagaOrchestrator.
 * Returns reservation ID on success, throws exception on failure.
 */
@Slf4j
@Service
public class OrchestrationInventoryService {

    private final Map<String, Integer> inventoryStore = new ConcurrentHashMap<>(Map.of(
            "PRODUCT_001", 100,
            "PRODUCT_002", 0,     // out of stock
            "PRODUCT_003", 50
    ));

    /**
     * Reserve inventory for an order.
     * Throws exception if insufficient stock — orchestrator will refund payment.
     */
    public String reserveInventory(String productId, int quantity) {
        log.info("[ORCHESTRATION] Reserving {} units of product: {}", quantity, productId);

        int available = inventoryStore.getOrDefault(productId, 0);

        if (available < quantity) {
            throw new RuntimeException(
                "Insufficient inventory for " + productId +
                ". Available: " + available + ", Required: " + quantity
            );
        }

        inventoryStore.put(productId, available - quantity);
        String reservationId = "RES-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("[ORCHESTRATION] Inventory RESERVED. ReservationId: {}", reservationId);
        return reservationId;
    }

    /**
     * Compensating transaction — release reserved inventory.
     */
    public void releaseInventory(String productId, int quantity) {
        log.info("[ORCHESTRATION] COMPENSATING — Releasing {} units of product: {}", quantity, productId);
        inventoryStore.merge(productId, quantity, Integer::sum);
        log.info("[ORCHESTRATION] Inventory released for product: {}", productId);
    }
}
