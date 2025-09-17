package com.iot.dashboardapi.controller;

import com.iot.dashboardapi.dto.OcupacionDTO;
import com.iot.dashboardapi.dto.SeriesPoint;
import com.iot.dashboardapi.model.Event;
import com.iot.dashboardapi.repository.EventRepository;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api")
public class DashboardController {

    private final EventRepository repo;

    public DashboardController(EventRepository repo) { this.repo = repo; }

    // GET /api/aulas/A101/ocupacion-actual
    @GetMapping("/aulas/{id}/ocupacion-actual")
    public OcupacionDTO ocupacionActual(@PathVariable("id") String aulaId) {
        Integer occ = repo.ocupacionActual(aulaId);
        return new OcupacionDTO(aulaId, occ == null ? 0 : occ);
    }

    // GET /api/aulas/A101/series?from=2025-09-10T10:00:00Z&to=2025-09-10T12:00:00Z
    @GetMapping("/aulas/{id}/series")
    public List<SeriesPoint> series(
            @PathVariable("id") String aulaId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {

        List<Event> events = repo.eventosEnRango(aulaId, from, to);
        List<SeriesPoint> out = new ArrayList<>(events.size());
        for (Event e : events) {
            int delta = "in".equalsIgnoreCase(e.getEv()) ? 1 : -1;
            out.add(new SeriesPoint(e.getTs(), delta));
        }
        return out;
    }
}
