package io.orqueio.example.hotel.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.orqueio.example.hotel.model.HotelPayload;
import io.orqueio.example.hotel.model.WorkerMessage;
import io.orqueio.example.hotel.model.WorkerResponse;
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
    private final KafkaTemplate<String, WorkerResponse> kafkaTemplate;
    private final ObjectMapper objectMapper;

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

    public HotelConsumer(KafkaTemplate<String, WorkerResponse> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "saga.hotel.command", groupId = "hotel-service")
    public void onCommand(WorkerMessage command) {
        try {
            HotelPayload payload = objectMapper.readValue(command.getPayload(), HotelPayload.class);

            if ("EXECUTE".equals(command.getAction())) {
                handleBooking(command.getCorrelationId(), payload);
            } else if ("COMPENSATE".equals(command.getAction())) {
                handleCancellation(command.getCorrelationId(), payload.getBookingId(), payload.getDestination());
            }
        } catch (Exception e) {
            log.error("[HOTEL] Error processing command", e);
        }
    }

    private void handleBooking(String correlationId, HotelPayload payload) {
        String bookingId = payload.getBookingId();
        String destination = payload.getDestination();
        double budget = payload.getBudget();

        log.info("[HOTEL] Booking hotel in {} for '{}', budget: {} EUR, booking {}",
                destination, payload.getTravelerName(), budget, bookingId);

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

        sendResponse(correlationId, bookingId, success, message);
    }

    private void handleCancellation(String correlationId, String bookingId, String destination) {
        log.info("[HOTEL-COMPENSATE] Cancelling hotel for booking {}", bookingId);

        String reserved = reservations.remove(bookingId);
        if (reserved != null) {
            availableRooms.merge(reserved, 1, Integer::sum);
            log.info("[HOTEL-COMPENSATE] Restored 1 room in {}", reserved);
        }

        sendResponse(correlationId, bookingId, true, "Hotel reservation cancelled");
    }

    private void sendResponse(String correlationId, String bookingId, boolean success, String message) {
        WorkerResponse response = new WorkerResponse(correlationId, success, message);
        kafkaTemplate.send("saga.hotel.response", bookingId, response);
    }
}
