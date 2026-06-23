package com.example.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Bank Saga Pattern — Spring Boot Application
 *
 * Demonstrates TWO Saga pattern implementations:
 *
 * 1. CHOREOGRAPHY SAGA (event-driven)
 *    - Services communicate via Kafka events
 *    - No central coordinator
 *    - POST /api/orders/choreography
 *
 * 2. ORCHESTRATION SAGA (centrally controlled)
 *    - SagaOrchestrator calls each service directly
 *    - Single point of control
 *    - POST /api/orders/orchestration
 *
 * H2 Console: http://localhost:8080/h2-console
 * API Base:   http://localhost:8080/api/orders
 */

@SpringBootApplication
@EnableKafka
public class SagaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SagaApplication.class, args);
	}

}
