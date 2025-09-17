package com.iot.ingestservice.repository;

import com.iot.ingestservice.model.Event;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventRepository extends JpaRepository<Event, Long> {}
