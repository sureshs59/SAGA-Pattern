package com.example.saga.order;

import jakarta.validation.constraints.*;
import lombok.*;
import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest {

    @NotBlank(message = "Customer ID is required")
    private String customerId;

    @NotBlank(message = "Product ID is required")
    private String productId;

    @NotNull @Min(1)
    private Integer quantity;

    @NotNull @DecimalMin("0.01")
    private BigDecimal totalAmount;
}
