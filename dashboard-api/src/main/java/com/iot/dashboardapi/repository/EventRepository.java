package com.iot.dashboardapi.repository;

import com.iot.dashboardapi.model.Event;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface EventRepository extends JpaRepository<Event, Long> {

    // Ocupación actual (in=+1, out=-1)
    @Query("""
    select coalesce(sum(case when e.ev = 'in' then 1 else -1 end), 0)
    from Event e
    where e.aulaId = :aula and e.ts <= CURRENT_TIMESTAMP
  """)
    Integer ocupacionActual(@Param("aula") String aula);

    // Serie de deltas (in=+1/out=-1) en un rango (para graficar)
    @Query("""
    select e from Event e
    where e.aulaId = :aula and e.ts between :from and :to
    order by e.ts asc
  """)
    List<Event> eventosEnRango(@Param("aula") String aula,
                               @Param("from") Instant from,
                               @Param("to") Instant to);
}
