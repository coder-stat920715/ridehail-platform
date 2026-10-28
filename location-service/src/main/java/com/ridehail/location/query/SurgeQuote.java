package com.ridehail.location.query;

import java.math.BigDecimal;

public record SurgeQuote(BigDecimal multiplier, int demand, int supply) { }
