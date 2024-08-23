package org.example.producer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import java.time.LocalDateTime;

@Data
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class Transaction {
    private String bank;
    private Long clientId;
    private TransactionType orderType;
    private Integer quantity;
    private Double price;
    private LocalDateTime createdAt;

    public enum TransactionType {
        INCOME,
        OUTCOME
    }
}
