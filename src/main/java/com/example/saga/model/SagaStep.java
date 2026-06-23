package com.example.saga.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity(name = "saga_steps")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaStep {
	@Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;
 
    @Column(nullable = false)
    private String orderId;
 
    @Column(nullable = false)
    private String stepName;  // e.g. "PAYMENT", "INVENTORY", "NOTIFICATION"
 
    @Enumerated(EnumType.STRING)
    private StepStatus status; // STARTED, COMPLETED, FAILED, COMPENSATING, COMPENSATED
 
    private String payload;       // JSON payload sent to the service
    private String response;      // JSON response received
    private String failureReason; // Why it failed
 
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
 
    public enum StepStatus {
        STARTED, COMPLETED, FAILED, COMPENSATING, COMPENSATED
    }
}
