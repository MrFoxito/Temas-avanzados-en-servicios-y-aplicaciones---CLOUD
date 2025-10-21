package com.iot.ingestservice.controller;

import com.iot.ingestservice.dto.ChirpRawDTO;
import com.iot.ingestservice.dto.ChirpUplinkDTO;
import com.iot.ingestservice.dto.MockEventDTO;
import com.iot.ingestservice.model.Event;
import com.iot.ingestservice.repository.EventRepository;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/ingest")
public class IngestController {

    private final EventRepository repo;
    private static final String API_KEY = "secret123"; // muévelo a config/env en serio

    public IngestController(EventRepository repo) { this.repo = repo; }

    @PostMapping("/mock")
    public Map<String,Object> ingestMock(@Valid @RequestBody MockEventDTO dto){
        Event e = new Event();
        e.setAulaId(dto.aula());
        e.setNodoId(dto.nodo());
        e.setEv(dto.ev());
        e.setTs(dto.ts() != null ? dto.ts() : Instant.now());
        e.setRssi(dto.rssi());
        e.setSnr(dto.snr());
        repo.save(e);
        return Map.of("status","ok","savedId", e.getId());
    }

    @PostMapping("/chirpstack")
    public Map<String,Object> ingestChirpstack(
            @RequestHeader("x-api-key") String apiKey,
            @RequestBody ChirpUplinkDTO uplink){

        if (!API_KEY.equals(apiKey)) {
            return Map.of("status","forbidden");
        }

        // Extrae datos del decoder (ajusta si usas otro esquema)
        var decoded = uplink.uplink_message().decoded_payload();
        var meta = uplink.uplink_message().rx_metadata() != null && !uplink.uplink_message().rx_metadata().isEmpty()
                ? uplink.uplink_message().rx_metadata().get(0) : null;

        Event e = new Event();
        e.setAulaId(decoded != null ? decoded.aula() : "UNK");
        e.setNodoId(decoded != null ? decoded.nodo() : uplink.end_device_ids().device_id());
        e.setEv(decoded != null ? decoded.ev() : "in");
        e.setTs(decoded != null && decoded.ts()!=null ? Instant.parse(decoded.ts()) : Instant.now());
        if (meta != null) { e.setRssi(meta.rssi()); e.setSnr(meta.snr()); }

        repo.save(e);
        return Map.of("status","ok","savedId", e.getId());
    }

    // =====================================================
    // 3️⃣ MOCK-RAW (formato actual de ChirpStack, simulado)
    // =====================================================
    @PostMapping("/mock-raw")
    public Map<String, Object> ingestMockRaw(@RequestBody ChirpRawDTO dto) {
        Event e = decodePayload(dto.payload_hex(), dto.timestamp(), dto.deviceName());
        repo.save(e);
        return Map.of("status", "ok", "savedId", e.getId());
    }

    // =====================================================
    // 4️⃣ CHIRPSTACK-RAW (Webhook real actual)
    // =====================================================
    @PostMapping("/chirpstack-raw")
    public Map<String, Object> ingestChirpstackRaw(
            @RequestHeader("x-api-key") String apiKey,
            @RequestBody ChirpRawDTO dto) {

        if (!API_KEY.equals(apiKey)) {
            return Map.of("status", "forbidden");
        }

        Event e = decodePayload(dto.payload_hex(), dto.timestamp(), dto.deviceName());
        repo.save(e);
        return Map.of("status", "ok", "savedId", e.getId());
    }

    // =====================================================
    // DECODIFICADOR genérico de payload HEX
    // =====================================================
    private Event decodePayload(String payloadHex, String timestamp, String deviceName) {
        Event e = new Event();
        e.setNodoId(deviceName != null ? deviceName : "UNK");
        e.setAulaId("A101");

        try {
            byte[] bytes = hexStringToByteArray(payloadHex);
            int eventType = bytes[0] & 0xFF;
            String ev = switch (eventType) {
                case 0x01 -> "in";
                case 0x02 -> "out";
                default -> "unk";
            };
            e.setEv(ev);
            e.setTs(timestamp != null ? Instant.parse(timestamp) : Instant.now());
        } catch (Exception ex) {
            e.setEv("unk");
            e.setTs(Instant.now());
        }

        return e;
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2)
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        return data;
    }
}
