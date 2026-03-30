package io.orqueio.example.flight.kafka;

import io.orqueio.example.flight.model.FlightPayload;
import io.orqueio.example.flight.model.WorkerMessage;
import io.orqueio.example.flight.model.WorkerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class FlightConsumer {

    private static final Logger log = LoggerFactory.getLogger(FlightConsumer.class);
    private final KafkaTemplate<String, WorkerResponse> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Simulated seat availability per destination
    private final ConcurrentHashMap<String, Integer> availableSeats = new ConcurrentHashMap<>(Map.of(
            "Paris", 120, "Tokyo", 80, "New York", 200,
            "London", 150, "Dubai", 60, "Sydney", 40
    ));
    private final ConcurrentHashMap<String, Integer> reservations = new ConcurrentHashMap<>();

    public FlightConsumer(KafkaTemplate<String, WorkerResponse> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "saga.flight.command", groupId = "flight-service")
    public void onCommand(WorkerMessage command) {
        try {
            FlightPayload payload = objectMapper.readValue(command.getPayload(), FlightPayload.class);

            if ("EXECUTE".equals(command.getAction())) {
                handleBooking(command.getCorrelationId(), payload);
            } else if ("COMPENSATE".equals(command.getAction())) {
                handleCancellation(command.getCorrelationId(), payload.getBookingId(), payload.getDestination());
            }
        } catch (Exception e) {
            log.error("[FLIGHT] Error processing command", e);
        }
    }

    private void handleBooking(String correlationId, FlightPayload payload) {
        String bookingId = payload.getBookingId();
        String destination = payload.getDestination();
        int passengers = payload.getPassengers();

        int seats = availableSeats.getOrDefault(destination, 0);
        boolean success = seats >= passengers;
        String message;

        if (success) {
            availableSeats.put(destination, seats - passengers);
            reservations.put(bookingId, passengers);
            String flightNumber = "FL-" + UUID.randomUUID().toString().substring(0, 4).toUpperCase();
            message = String.format("Flight %s booked: %d seat(s) to %s. Remaining: %d",
                    flightNumber, passengers, destination, seats - passengers);
            log.info("[FLIGHT] {} - {}", bookingId, message);
        } else {
            message = String.format("No flights available to %s. Requested: %d, Available: %d",
                    destination, passengers, seats);
            log.warn("[FLIGHT] {} - {}", bookingId, message);
        }

        sendResponse(correlationId, bookingId, success, message);
    }

    private void handleCancellation(String correlationId, String bookingId, String destination) {
        Integer reserved = reservations.remove(bookingId);
        if (reserved != null) {
            availableSeats.merge(destination, reserved, Integer::sum);
            log.info("[FLIGHT-COMPENSATE] {} - Restored {} seat(s) to {}", bookingId, reserved, destination);
        }
    }

    private void sendResponse(String correlationId, String bookingId, boolean success, String message) {
        WorkerResponse response = new WorkerResponse(correlationId, success, message);
        kafkaTemplate.send("saga.flight.response", bookingId, response);
    }
}
