package io.orqueio.example.carrental.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.orqueio.example.carrental.model.CarRentalPayload;
import io.orqueio.example.carrental.model.WorkerMessage;
import io.orqueio.example.carrental.model.WorkerResponse;
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
    private final KafkaTemplate<String, WorkerResponse> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Simulated car availability per destination
    private final ConcurrentHashMap<String, Integer> availableCars = new ConcurrentHashMap<>(Map.of(
            "Paris", 15, "Tokyo", 10, "New York", 25,
            "London", 18, "Dubai", 8, "Sydney", 12
    ));
    private final ConcurrentHashMap<String, String> reservations = new ConcurrentHashMap<>();

    public CarRentalConsumer(KafkaTemplate<String, WorkerResponse> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "saga.car.command", groupId = "car-rental-service")
    public void onCommand(WorkerMessage command) {
        try {
            CarRentalPayload payload = objectMapper.readValue(command.getPayload(), CarRentalPayload.class);

            if ("EXECUTE".equals(command.getAction())) {
                handleRental(command.getCorrelationId(), payload);
            } else if ("COMPENSATE".equals(command.getAction())) {
                handleCancellation(command.getCorrelationId(), payload.getBookingId(), payload.getDestination());
            }
        } catch (Exception e) {
            log.error("[CAR-RENTAL] Error processing command", e);
        }
    }

    private void handleRental(String correlationId, CarRentalPayload payload) {
        String bookingId = payload.getBookingId();
        String destination = payload.getDestination();

        log.info("[CAR-RENTAL] Renting car in {} for '{}' ({} to {}), booking {}",
                destination, payload.getTravelerName(), payload.getDepartureDate(),
                payload.getReturnDate(), bookingId);

        int cars = availableCars.getOrDefault(destination, 0);
        boolean success = cars > 0;
        String message;

        if (success) {
            availableCars.put(destination, cars - 1);
            reservations.put(bookingId, destination);
            String rentalCode = "CAR-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
            message = String.format("Car rented in %s. Confirmation: %s. Pickup: %s, Return: %s",
                    destination, rentalCode, payload.getDepartureDate(), payload.getReturnDate());
            log.info("[CAR-RENTAL] {} - {}", bookingId, message);
        } else {
            message = String.format("No cars available in %s", destination);
            log.warn("[CAR-RENTAL] {} - {}", bookingId, message);
        }

        sendResponse(correlationId, bookingId, success, message);
    }

    private void handleCancellation(String correlationId, String bookingId, String destination) {
        log.info("[CAR-RENTAL-COMPENSATE] Cancelling car rental for booking {}", bookingId);

        String reserved = reservations.remove(bookingId);
        if (reserved != null) {
            availableCars.merge(reserved, 1, Integer::sum);
            log.info("[CAR-RENTAL-COMPENSATE] Restored 1 car in {}", reserved);
        }

        sendResponse(correlationId, bookingId, true, "Car rental cancelled");
    }

    private void sendResponse(String correlationId, String bookingId, boolean success, String message) {
        WorkerResponse response = new WorkerResponse(correlationId, success, message);
        kafkaTemplate.send("saga.car.response", bookingId, response);
    }
}
