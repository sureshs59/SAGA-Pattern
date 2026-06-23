//package com.example.saga.orchestration;
//
//
//import com.example.saga.entity.Order;
//import com.example.saga.model.OrderStatus;
//import com.example.saga.model.SagaStep;
//import com.example.saga.order.OrderRepository;
//import com.example.saga.order.OrderService;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.ArgumentCaptor;
//import org.mockito.Captor;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import java.math.BigDecimal;
//import java.util.List;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.mockito.ArgumentMatchers.*;
//import static org.mockito.Mockito.*;
//
///**
// * Unit tests for Orchestration Saga.
// *
// * Tests cover:
// *  1. Happy path — all steps succeed
// *  2. Payment failure — saga cancels immediately
// *  3. Inventory failure — saga compensates payment and cancels
// *  4. Notification failure — saga completes (non-critical step)
// */
//@ExtendWith(MockitoExtension.class)
//@DisplayName("Saga Orchestrator Tests")
//class SagaOrchestratorTest {
//
//    @Mock private OrderService orderService;
//    @Mock private SagaStepRepository sagaStepRepository;
//    @Mock private OrchestrationPaymentService paymentService;
//    @Mock private OrchestrationInventoryService inventoryService;
//    @Mock private OrchestrationNotificationService notificationService;
//
//    @InjectMocks
//    private SagaOrchestrator orchestrator;
//
//    @Captor
//    private ArgumentCaptor<SagaStep> sagaStepCaptor;
//
//    private Order testOrder;
//
//    @BeforeEach
//    void setUp() {
//        testOrder = Order.builder()
//                .id("ORDER-001")
//                .customerId("CUST-001")
//                .productId("PRODUCT-001")
//                .quantity(2)
//                .totalAmount(new BigDecimal("500.00"))
//                .status(OrderStatus.PENDING)
//                .sagaType("ORCHESTRATION")
//                .build();
//
//        // Mock SagaStep save to return the step passed in
//        when(sagaStepRepository.save(any(SagaStep.class)))
//                .thenAnswer(inv -> inv.getArgument(0));
//
//        // Mock findById for compensation scenarios
//        when(orderService.findById("ORDER-001")).thenReturn(testOrder);
//    }
//
//    @Test
//    @DisplayName("Happy path: all steps succeed → order COMPLETED")
//    void testHappyPath() throws Exception {
//        // Arrange
//        when(paymentService.processPayment(anyString(), any())).thenReturn("TXN-123");
//        when(inventoryService.reserveInventory(anyString(), anyInt())).thenReturn("RES-456");
//        doNothing().when(notificationService).sendOrderConfirmation(anyString(), anyString());
//
//        Order completedOrder = Order.builder()
//                .id("ORDER-001").status(OrderStatus.COMPLETED).build();
//        when(orderService.findById("ORDER-001")).thenReturn(completedOrder);
//
//        // Act
//        Order result = orchestrator.executeSaga(testOrder);
//
//        // Assert
//        assertThat(result.getStatus()).isEqualTo(OrderStatus.COMPLETED);
//
//        // Verify all 3 steps were executed
//        verify(paymentService).processPayment("CUST-001", new BigDecimal("500.00"));
//        verify(inventoryService).reserveInventory("PRODUCT-001", 2);
//        verify(notificationService).sendOrderConfirmation("CUST-001", "ORDER-001");
//
//        // Verify order was marked complete
//        verify(orderService).completeOrder("ORDER-001");
//
//        // Verify no compensation happened
//        verify(paymentService, never()).refundPayment(anyString());
//        verify(inventoryService, never()).releaseInventory(anyString(), anyInt());
//    }
//
//    @Test
//    @DisplayName("Payment failure: saga cancels without compensation")
//    void testPaymentFailure() throws Exception {
//        // Arrange — payment throws exception
//        when(paymentService.processPayment(anyString(), any()))
//                .thenThrow(new RuntimeException("Insufficient funds"));
//
//        Order cancelledOrder = Order.builder()
//                .id("ORDER-001").status(OrderStatus.CANCELLED)
//                .failureReason("Payment failed: Insufficient funds").build();
//        when(orderService.findById("ORDER-001")).thenReturn(cancelledOrder);
//
//        // Act
//        Order result = orchestrator.executeSaga(testOrder);
//
//        // Assert
//        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
//
//        // Verify payment was attempted
//        verify(paymentService).processPayment("CUST-001", new BigDecimal("500.00"));
//
//        // Verify inventory and notification were NOT called (saga stopped early)
//        verify(inventoryService, never()).reserveInventory(anyString(), anyInt());
//        verify(notificationService, never()).sendOrderConfirmation(anyString(), anyString());
//
//        // Verify NO compensation needed (payment failed = no money taken)
//        verify(paymentService, never()).refundPayment(anyString());
//
//        // Verify order cancelled
//        verify(orderService).cancelOrder(eq("ORDER-001"), contains("Payment failed"));
//    }
//
//    @Test
//    @DisplayName("Inventory failure: saga compensates payment → order CANCELLED")
//    void testInventoryFailure_compensatesPayment() throws Exception {
//        // Arrange
//        when(paymentService.processPayment(anyString(), any())).thenReturn("TXN-123");
//        when(inventoryService.reserveInventory(anyString(), anyInt()))
//                .thenThrow(new RuntimeException("Insufficient inventory"));
//
//        Order cancelledOrder = Order.builder()
//                .id("ORDER-001").status(OrderStatus.CANCELLED)
//                .failureReason("Inventory failed").build();
//        when(orderService.findById("ORDER-001")).thenReturn(cancelledOrder);
//
//        // Act
//        Order result = orchestrator.executeSaga(testOrder);
//
//        // Assert
//        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
//
//        // Verify payment was processed
//        verify(paymentService).processPayment("CUST-001", new BigDecimal("500.00"));
//
//        // Verify inventory was attempted
//        verify(inventoryService).reserveInventory("PRODUCT-001", 2);
//
//        // Verify COMPENSATION — payment was refunded
//        verify(paymentService).refundPayment("TXN-123");
//
//        // Verify notification NOT sent (order cancelled)
//        verify(notificationService, never()).sendOrderConfirmation(anyString(), anyString());
//
//        // Verify order cancelled
//        verify(orderService).cancelOrder(eq("ORDER-001"), contains("Inventory failed"));
//    }
//
//    @Test
//    @DisplayName("Notification failure: saga still COMPLETES (non-critical step)")
//    void testNotificationFailure_sagaStillCompletes() throws Exception {
//        // Arrange
//        when(paymentService.processPayment(anyString(), any())).thenReturn("TXN-123");
//        when(inventoryService.reserveInventory(anyString(), anyInt())).thenReturn("RES-456");
//        doThrow(new RuntimeException("Email server down"))
//                .when(notificationService).sendOrderConfirmation(anyString(), anyString());
//
//        Order completedOrder = Order.builder()
//                .id("ORDER-001").status(OrderStatus.COMPLETED).build();
//        when(orderService.findById("ORDER-001")).thenReturn(completedOrder);
//
//        // Act
//        Order result = orchestrator.executeSaga(testOrder);
//
//        // Assert — saga still completes despite notification failure
//        assertThat(result.getStatus()).isEqualTo(OrderStatus.COMPLETED);
//
//        // Verify no compensation happened
//        verify(paymentService, never()).refundPayment(anyString());
//        verify(inventoryService, never()).releaseInventory(anyString(), anyInt());
//
//        // Verify order still completed
//        verify(orderService).completeOrder("ORDER-001");
//    }
//
//    @Test
//    @DisplayName("Saga log: all steps are recorded in correct sequence")
//    void testSagaStepsAreLogged() throws Exception {
//        // Arrange
//        when(paymentService.processPayment(anyString(), any())).thenReturn("TXN-123");
//        when(inventoryService.reserveInventory(anyString(), anyInt())).thenReturn("RES-456");
//
//        Order completedOrder = Order.builder()
//                .id("ORDER-001").status(OrderStatus.COMPLETED).build();
//        when(orderService.findById("ORDER-001")).thenReturn(completedOrder);
//
//        // Act
//        orchestrator.executeSaga(testOrder);
//
//        // Capture all saved saga steps
//        verify(sagaStepRepository, atLeast(3)).save(sagaStepCaptor.capture());
//        List<SagaStep> savedSteps = sagaStepCaptor.getAllValues();
//
//        // Verify step names were logged
//        assertThat(savedSteps)
//                .extracting(SagaStep::getStepName)
//                .contains("PAYMENT", "INVENTORY", "NOTIFICATION");
//    }
//}