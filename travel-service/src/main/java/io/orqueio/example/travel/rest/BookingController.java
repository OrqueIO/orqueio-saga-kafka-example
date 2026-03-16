package io.orqueio.example.travel.rest;

import io.orqueio.bpm.engine.RuntimeService;
import io.orqueio.bpm.engine.runtime.ProcessInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/bookings")
public class BookingController {

    private static final Logger log = LoggerFactory.getLogger(BookingController.class);
    private final RuntimeService runtimeService;

    public BookingController(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> createBooking(@RequestBody Map<String, Object> request) {
        String bookingId = "TRV-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Map<String, Object> variables = new HashMap<>();
        variables.put("bookingId", bookingId);
        variables.put("travelerName", request.getOrDefault("travelerName", "John Doe"));
        variables.put("destination", request.getOrDefault("destination", "Paris"));
        variables.put("departureDate", request.getOrDefault("departureDate", "2026-06-01"));
        variables.put("returnDate", request.getOrDefault("returnDate", "2026-06-07"));
        variables.put("passengers", ((Number) request.getOrDefault("passengers", 1)).intValue());
        variables.put("budget", ((Number) request.getOrDefault("budget", 2000.0)).doubleValue());

        log.info("[BOOKING] Creating travel booking {} for {} to {}",
                bookingId, variables.get("travelerName"), variables.get("destination"));

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", bookingId, variables);

        Map<String, Object> response = new HashMap<>();
        response.put("bookingId", bookingId);
        response.put("processInstanceId", processInstance.getId());
        response.put("status", "SAGA_STARTED");
        response.put("message", "Travel booking saga initiated");

        return ResponseEntity.ok(response);
    }

    @GetMapping("/{bookingId}/status")
    public ResponseEntity<Map<String, Object>> getBookingStatus(@PathVariable String bookingId) {
        var instances = runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(bookingId)
                .list();

        Map<String, Object> response = new HashMap<>();
        response.put("bookingId", bookingId);

        if (instances.isEmpty()) {
            response.put("status", "COMPLETED_OR_NOT_FOUND");
            response.put("message", "Process completed or booking not found");
        } else {
            var instance = instances.get(0);
            response.put("status", "IN_PROGRESS");
            response.put("processInstanceId", instance.getId());
            response.put("suspended", instance.isSuspended());
        }

        return ResponseEntity.ok(response);
    }
}
