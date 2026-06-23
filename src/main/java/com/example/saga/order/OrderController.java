package com.example.saga.order;


import com.example.saga.entity.Order;
import com.example.saga.model.OrderStatus;
import com.example.saga.model.SagaStep;
import com.example.saga.orchestration.SagaOrchestrator;
import com.example.saga.orchestration.SagaStepRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST Controller — exposes endpoints to trigger both saga patterns.
 *
 * POST /api/orders/choreography   — starts Choreography Saga (event-driven)
 * POST /api/orders/orchestration  — starts Orchestration Saga (direct calls)
 * GET  /api/orders/{id}           — get order status
 * GET  /api/orders/{id}/saga-log  — get saga step log (orchestration only)
 */
@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final SagaOrchestrator sagaOrchestrator;
    private final SagaStepRepository sagaStepRepository;
    private final OrderRepository orderRepository;

    /**
     * CHOREOGRAPHY SAGA — creates order and publishes event.
     * Services react autonomously through Kafka.
     *
     * Test happy path:    customerId = "CUST_001"
     * Test payment fail:  customerId = "CUST_FAIL"
     * Test inventory fail: productId = "PRODUCT_002" (out of stock)
     */
    @PostMapping("/choreography")
    public ResponseEntity<Map<String, Object>> createOrderChoreography(
            @Valid @RequestBody OrderRequest request) {

        log.info("[API] Creating order via CHOREOGRAPHY saga");
        Order order = orderService.createOrderChoreography(request);

        return ResponseEntity.ok(Map.of(
            "orderId",   order.getId(),
            "status",    order.getStatus(),
            "sagaType",  "CHOREOGRAPHY",
            "message",   "Order created. Processing asynchronously via Kafka events.",
            "checkStatus", "/api/orders/" + order.getId()
        ));
    }

    /**
     * ORCHESTRATION SAGA — creates order and orchestrator drives all steps.
     * Synchronous — response contains final status.
     *
     * Test happy path:    customerId = "CUST_001", productId = "PRODUCT_001"
     * Test payment fail:  customerId = "CUST_FAIL"
     * Test inventory fail: productId = "PRODUCT_002"
     */
    @PostMapping("/orchestration")
    public ResponseEntity<Map<String, Object>> createOrderOrchestration(
            @Valid @RequestBody OrderRequest request) {

        log.info("[API] Creating order via ORCHESTRATION saga");

        // Create the order entity first
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(request.getTotalAmount())
                .status(OrderStatus.PENDING)
                .sagaType("ORCHESTRATION")
                .paymentCompensated(false)
                .inventoryCompensated(false)
                .build();

        order = orderRepository.save(order);

        // Orchestrator drives all saga steps synchronously
        Order result = sagaOrchestrator.executeSaga(order);

        return ResponseEntity.ok(Map.of(
            "orderId",      result.getId(),
            "status",       result.getStatus(),
            "sagaType",     "ORCHESTRATION",
            "success",      result.getStatus() == OrderStatus.COMPLETED,
            "failureReason", result.getFailureReason() != null ? result.getFailureReason() : "",
            "sagaLog",      "/api/orders/" + result.getId() + "/saga-log"
        ));
    }

    /**
     * Get current order status.
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable String orderId) {
        return ResponseEntity.ok(orderService.findById(orderId));
    }

    /**
     * Get saga step log for an order (Orchestration saga).
     * Shows every step, its status, and compensation details.
     */
    @GetMapping("/{orderId}/saga-log")
    public ResponseEntity<List<SagaStep>> getSagaLog(@PathVariable String orderId) {
        List<SagaStep> steps = sagaStepRepository.findByOrderIdOrderByStartedAtAsc(orderId);
        return ResponseEntity.ok(steps);
    }

    /**
     * List all orders.
     */
    @GetMapping
    public ResponseEntity<List<Order>> listOrders() {
        return ResponseEntity.ok(orderRepository.findAll());
    }
}
