package com.iot.ingestservice.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record ChirpUplinkDTO(
        EndDeviceIds end_device_ids,
        UplinkMessage uplink_message
){
    public record EndDeviceIds(String device_id, String dev_eui) {}
    public record UplinkMessage(
            DecodedPayload decoded_payload, List<RxMetadata> rx_metadata, Long received_at // opcional
    ){}
    public record DecodedPayload(
            String aula, String nodo, String ev, String ts  // si tu codec ya emite estos campos
    ){}
    public record RxMetadata(Double rssi, Double snr) {}
}
