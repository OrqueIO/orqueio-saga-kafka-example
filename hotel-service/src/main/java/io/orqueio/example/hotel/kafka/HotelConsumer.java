package io.orqueio.example.hotel.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class HotelConsumer {

    private static final Logger log = LoggerFactory.getLogger(HotelConsumer.class);
    private final KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    // Simulated room rates per destination (per night in EUR)
    private final ConcurrentHashMap<String, Double> roomRates = new ConcurrentHashMap<>(Map.of(
            "Paris", 180.0, "Tokyo", 220.0, "New York", 350.0,
            "London", 280.0, "Dubai", 450.0, "Sydney", 200.0
    ));
    private final ConcurrentHashMap<String, Integer> availableRooms = new ConcurrentHashMap<>(Map.of(
            "Paris", 25, "Tokyo", 15, "New York", 30,
            "London", 20, "Dubai", 10, "Sydney", 12
    ));
    private final ConcurrentHashMap<String, String> reservations = new ConcurrentHashMap<>();

    public HotelConsumer(KafkaTemplate<String, Map<String, Object>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "saga.hotel.command", groupId = "hotel-service")
    public void onCommand(Map<String, Object> command) {
        String action = (String) command.get("action");
        String bookingId = (String) command.get("bookingId");
        String processInstanceId = (String) command.get("processInstanceId");
        String travelerName = (String) command.getOrDefault("travelerName", "Unknown");
        String destination = (String) command.getOrDefault("destination", "Paris");
        double budget = ((Number) command.getOrDefault("budget", 2000.0)).doubleValue();

        if ("EXECUTE".equals(action)) {
            handleBooking(bookingId, processInstanceId, travelerName, destination, budget);
        } else if ("COMPENSATE".equals(action)) {
            handleCancellation(bookingId, processInstanceId, destination);
        }
    }

    private void handleBooking(String bookingId, String processInstanceId,
                               String travelerName, String destination, double budget) {
        log.info("[HOTEL] Booking hotel in {} for '{}', budget: {} EUR, booking {}",
                destination, travelerName, budget, bookingId);

        double rate = roomRates.getOrDefault(destination, 150.0);
        int rooms = availableRooms.getOrDefault(destination, 0);
        boolean success;
        String message;

        if (rooms <= 0) {
            success = false;
            message = String.format("No rooms available in %s", destination);
            log.warn("[HOTEL] {} - {}", bookingId, message);
        } else if (budget < rate) {
            success = false;
            message = String.format("Budget %.2f EUR insufficient for %s (rate: %.2f EUR/night)",
                    budget, destination, rate);
            log.warn("[HOTEL] {} - {}", bookingId, message);
        } else {
            success = true;
            availableRooms.put(destination, rooms - 1);
            reservations.put(bookingId, destination);
            String confirmationCode = "HTL-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
            message = String.format("Hotel booked in %s. Confirmation: %s. Rate: %.2f EUR/night",
                    destination, confirmationCode, rate);
            log.info("[HOTEL] {} - {}", bookingId, message);
        }

        sendResponse(bookingId, processInstanceId, success, message);
    }

    private void handleCancellation(String bookingId, String processInstanceId, String destination) {
        log.info("[HOTEL-COMPENSATE] Cancelling hotel for booking {}", bookingId);

        String reserved = reservations.remove(bookingId);
        if (reserved != null) {
            availableRooms.merge(reserved, 1, Integer::sum);
            log.info("[HOTEL-COMPENSATE] Restored 1 room in {}", reserved);
        }

        sendResponse(bookingId, processInstanceId, true, "Hotel reservation cancelled");
    }

    private void sendResponse(String bookingId, String processInstanceId, boolean success, String message) {
        Map<String, Object> response = Map.of(
                "bookingId", bookingId, "processInstanceId", processInstanceId,
                "success", success, "message", message, "serviceName", "hotel-service"
        );
        kafkaTemplate.send("saga.hotel.response", bookingId, response);
    }
}
