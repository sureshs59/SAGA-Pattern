package com.example.saga.order;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.saga.entity.Order;
import com.example.saga.model.OrderStatus;

public interface OrderRepository extends JpaRepository<Order, Long> {
	List<Order> findByCustomerId(String customerId);
    List<Order> findByStatus(OrderStatus status);
    Order findById(String orderId);

}
