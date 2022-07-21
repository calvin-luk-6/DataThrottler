package com.marketdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@ToString
public class MarketData {
    private String symbol;
    private BigDecimal price;
    private long updateTime; // epoch time
}
