package com.iot.ingestservice.controller;

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
}
