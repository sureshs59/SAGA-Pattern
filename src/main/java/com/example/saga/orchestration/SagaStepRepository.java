package com.example.saga.orchestration;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.saga.model.SagaStep;

public interface SagaStepRepository extends JpaRepository<SagaStep, Long> {
	List<SagaStep> findByOrderIdOrderByStartedAtAsc(String orderId);
    List<SagaStep> findByStatus(SagaStep.StepStatus status);

}
