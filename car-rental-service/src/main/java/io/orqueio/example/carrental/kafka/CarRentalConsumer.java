package io.orqueio.example.carrental.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CarRentalConsumer {

    private static final Logger log = LoggerFactory.getLogger(CarRentalConsumer.class);
    private final KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    // Simulated car availability per destination
    private final ConcurrentHashMap<String, Integer> availableCars = new ConcurrentHashMap<>(Map.of(
            "Paris", 15, "Tokyo", 10, "New York", 25,
            "London", 18, "Dubai", 8, "Sydney", 12
    ));
    private final ConcurrentHashMap<String, String> reservations = new ConcurrentHashMap<>();

    public CarRentalConsumer(KafkaTemplate<String, Map<String, Object>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "saga.car.command", groupId = "car-rental-service")
    public void onCommand(Map<String, Object> command) {
        String action = (String) command.get("action");
        String bookingId = (String) command.get("bookingId");
        String processInstanceId = (String) command.get("processInstanceId");
        String travelerName = (String) command.getOrDefault("travelerName", "Unknown");
        String destination = (String) command.getOrDefault("destination", "Paris");
        String departureDate = (String) command.getOrDefault("departureDate", "");
        String returnDate = (String) command.getOrDefault("returnDate", "");

        if ("EXECUTE".equals(action)) {
            handleRental(bookingId, processInstanceId, travelerName, destination, departureDate, returnDate);
        } else if ("COMPENSATE".equals(action)) {
            handleCancellation(bookingId, processInstanceId, destination);
        }
    }

    private void handleRental(String bookingId, String processInstanceId,
                              String travelerName, String destination,
                              String departureDate, String returnDate) {
        log.info("[CAR-RENTAL] Renting car in {} for '{}' ({} to {}), booking {}",
                destination, travelerName, departureDate, returnDate, bookingId);

        int cars = availableCars.getOrDefault(destination, 0);
        boolean success = cars > 0;
        String message;

        if (success) {
            availableCars.put(destination, cars - 1);
            reservations.put(bookingId, destination);
            String rentalCode = "CAR-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
            message = String.format("Car rented in %s. Confirmation: %s. Pickup: %s, Return: %s",
                    destination, rentalCode, departureDate, returnDate);
            log.info("[CAR-RENTAL] {} - {}", bookingId, message);
        } else {
            message = String.format("No cars available in %s", destination);
            log.warn("[CAR-RENTAL] {} - {}", bookingId, message);
        }

        sendResponse(bookingId, processInstanceId, success, message);
    }

    private void handleCancellation(String bookingId, String processInstanceId, String destination) {
        log.info("[CAR-RENTAL-COMPENSATE] Cancelling car rental for booking {}", bookingId);

        String reserved = reservations.remove(bookingId);
        if (reserved != null) {
            availableCars.merge(reserved, 1, Integer::sum);
            log.info("[CAR-RENTAL-COMPENSATE] Restored 1 car in {}", reserved);
        }

        sendResponse(bookingId, processInstanceId, true, "Car rental cancelled");
    }

    private void sendResponse(String bookingId, String processInstanceId, boolean success, String message) {
        Map<String, Object> response = Map.of(
                "bookingId", bookingId, "processInstanceId", processInstanceId,
                "success", success, "message", message, "serviceName", "car-rental-service"
        );
        kafkaTemplate.send("saga.car.response", bookingId, response);
    }
}
