package com.example.saga.order;


import com.example.saga.entity.Order;
import com.example.saga.model.*;
import com.example.saga.model.SagaEvents.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Order Service — creates orders and kicks off the Saga.
 *
 * For CHOREOGRAPHY saga:
 *   - Saves the order and publishes OrderCreatedEvent to Kafka
 *   - Each downstream service picks up the event and does its work
 *
 * For ORCHESTRATION saga:
 *   - Saves the order and delegates to SagaOrchestrator
 *   - Orchestrator calls each service in sequence
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.order-created}")    private String orderCreatedTopic;
    @Value("${kafka.topic.order-cancelled}")  private String orderCancelledTopic;

    /**
     * Creates an order and starts the CHOREOGRAPHY saga.
     * Simply persists the order and publishes an event.
     * Services react to the event autonomously.
     */
    @Transactional
    public Order createOrderChoreography(OrderRequest request) {
        log.info("Creating order (CHOREOGRAPHY) for customer: {}", request.getCustomerId());

        // 1. Persist the order
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(request.getTotalAmount())
                .status(OrderStatus.PAYMENT_PENDING)
                .sagaType("CHOREOGRAPHY")
                .paymentCompensated(false)
                .inventoryCompensated(false)
                .build();

        order = orderRepository.save(order);
        log.info("Order saved with id: {}", order.getId());

        // 2. Publish event — Payment Service will react to this
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(order.getId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .totalAmount(order.getTotalAmount())
                .timestamp(LocalDateTime.now())
                .build();

        kafkaTemplate.send(orderCreatedTopic, order.getId(), event);
        log.info("Published OrderCreatedEvent for orderId: {}", order.getId());

        return order;
    }

    /**
     * Called by the Choreography saga when all steps succeed.
     */
    @Transactional
    public void completeOrder(String orderId) {
        Order order = findById(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        orderRepository.save(order);
        log.info("Order {} marked as COMPLETED", orderId);
    }

    /**
     * Called during compensation — cancels the order and notifies.
     */
    @Transactional
    public void cancelOrder(String orderId, String reason) {
        Order order = findById(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        orderRepository.save(order);
        log.warn("Order {} CANCELLED. Reason: {}", orderId, reason);

        // Publish cancellation event for notification service
        OrderCancelledEvent event = OrderCancelledEvent.builder()
                .orderId(orderId)
                .customerId(order.getCustomerId())
                .reason(reason)
                .timestamp(LocalDateTime.now())
                .build();
        kafkaTemplate.send(orderCancelledTopic, orderId, event);
    }

    @Transactional
    public void updateOrderStatus(String orderId, OrderStatus status) {
        Order order = findById(orderId);
        order.setStatus(status);
        orderRepository.save(order);
    }

    public Order findById(String orderId) {
        return orderRepository.findById(orderId);                
    }
}