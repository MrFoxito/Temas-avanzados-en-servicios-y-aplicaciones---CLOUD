package com.iot.dashboardapi.dto;

import java.time.Instant;

public record SeriesPoint(Instant ts, int delta) {}
