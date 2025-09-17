package com.iot.ingestservice.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;

@Data
@Entity @Table(name="events")
public class Event {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String aulaId;
    private String nodoId;

    @Column(nullable = false)
    private String ev; // "in" | "out"

    @Column(nullable = false)
    private Instant ts;

    private Double rssi;
    private Double snr;

}