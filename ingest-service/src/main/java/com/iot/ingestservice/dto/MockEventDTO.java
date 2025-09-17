package com.iot.ingestservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

public record MockEventDTO(
        @NotBlank String aula,
        @NotBlank String nodo,
        @NotBlank String ev,        // "in" | "out"
        @NotNull  Instant ts,
        Double rssi,
        Double snr
) {}
