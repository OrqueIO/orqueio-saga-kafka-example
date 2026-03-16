package io.orqueio.example.flight.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class FlightConsumer {

    private static final Logger log = LoggerFactory.getLogger(FlightConsumer.class);
    private final KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    // Simulated seat availability per destination
    private final ConcurrentHashMap<String, Integer> availableSeats = new ConcurrentHashMap<>(Map.of(
            "Paris", 120, "Tokyo", 80, "New York", 200,
            "London", 150, "Dubai", 60, "Sydney", 40
    ));
    private final ConcurrentHashMap<String, Integer> reservations = new ConcurrentHashMap<>();

    public FlightConsumer(KafkaTemplate<String, Map<String, Object>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "saga.flight.command", groupId = "flight-service")
    public void onCommand(Map<String, Object> command) {
        String action = (String) command.get("action");
        String bookingId = (String) command.get("bookingId");
        String processInstanceId = (String) command.get("processInstanceId");
        String travelerName = (String) command.getOrDefault("travelerName", "Unknown");
        String destination = (String) command.getOrDefault("destination", "Paris");
        int passengers = ((Number) command.getOrDefault("passengers", 1)).intValue();

        if ("EXECUTE".equals(action)) {
            handleBooking(bookingId, processInstanceId, travelerName, destination, passengers);
        } else if ("COMPENSATE".equals(action)) {
            handleCancellation(bookingId, processInstanceId, destination);
        }
    }

    private void handleBooking(String bookingId, String processInstanceId,
                               String travelerName, String destination, int passengers) {
        log.info("[FLIGHT] Booking {} seat(s) to {} for '{}', booking {}",
                passengers, destination, travelerName, bookingId);

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

        sendResponse(bookingId, processInstanceId, success, message);
    }

    private void handleCancellation(String bookingId, String processInstanceId, String destination) {
        log.info("[FLIGHT-COMPENSATE] Cancelling flight for booking {}", bookingId);

        Integer reserved = reservations.remove(bookingId);
        if (reserved != null) {
            availableSeats.merge(destination, reserved, Integer::sum);
            log.info("[FLIGHT-COMPENSATE] Restored {} seat(s) to {}", reserved, destination);
        }

        sendResponse(bookingId, processInstanceId, true, "Flight cancelled");
    }

    private void sendResponse(String bookingId, String processInstanceId, boolean success, String message) {
        Map<String, Object> response = Map.of(
                "bookingId", bookingId, "processInstanceId", processInstanceId,
                "success", success, "message", message, "serviceName", "flight-service"
        );
        kafkaTemplate.send("saga.flight.response", bookingId, response);
    }
}
