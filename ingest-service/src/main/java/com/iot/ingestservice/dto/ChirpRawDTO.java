package com.iot.ingestservice.dto;

public record ChirpRawDTO(
        String timestamp,
        String deviceName,
        String devEUI,
        Integer fCnt,
        Integer fPort,
        String payload_base64,
        String payload_hex
) {}
