package com.iot.dashboardapi.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;

@Data
@Entity @Table(name = "events")
public class Event {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String aulaId;
    private String nodoId;
    private String ev;   // "in" | "out"
    private Instant ts;
    private Double rssi;
    private Double snr;

    // getters/setters (o Lombok @Data)
}
