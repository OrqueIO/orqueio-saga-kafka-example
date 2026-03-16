package io.orqueio.example.flight.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class FlightConsumerTest {

    private FlightConsumer flightConsumer;
    private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        flightConsumer = new FlightConsumer(kafkaTemplate);
    }

    @Test
    @DisplayName("EXECUTE: should book flight when seats are available")
    void shouldBookFlight_whenSeatsAvailable() {
        // Given - Paris has 120 seats
        Map<String, Object> command = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-001",
                "processInstanceId", "proc-001",
                "travelerName", "Alice",
                "destination", "Paris",
                "passengers", 2
        );

        // When
        flightConsumer.onCommand(command);

        // Then - should send success response
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate).send(eq("saga.flight.response"), eq("TRV-001"), responseCaptor.capture());

        Map<String, Object> response = responseCaptor.getValue();
        assertTrue((Boolean) response.get("success"));
        assertEquals("TRV-001", response.get("bookingId"));
        assertEquals("proc-001", response.get("processInstanceId"));
        assertEquals("flight-service", response.get("serviceName"));
    }

    @Test
    @DisplayName("EXECUTE: should reject flight when destination unknown (0 seats)")
    void shouldRejectFlight_whenDestinationUnknown() {
        // Given - "Mars" has 0 seats
        Map<String, Object> command = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-002",
                "processInstanceId", "proc-002",
                "travelerName", "Diana",
                "destination", "Mars",
                "passengers", 1
        );

        // When
        flightConsumer.onCommand(command);

        // Then - should send failure response
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate).send(eq("saga.flight.response"), eq("TRV-002"), responseCaptor.capture());

        Map<String, Object> response = responseCaptor.getValue();
        assertFalse((Boolean) response.get("success"));
        assertTrue(((String) response.get("message")).contains("Mars"));
    }

    @Test
    @DisplayName("EXECUTE: should reject flight when not enough seats")
    void shouldRejectFlight_whenNotEnoughSeats() {
        // Given - Sydney has 40 seats, requesting 50
        Map<String, Object> command = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-003",
                "processInstanceId", "proc-003",
                "travelerName", "Group",
                "destination", "Sydney",
                "passengers", 50
        );

        // When
        flightConsumer.onCommand(command);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate).send(eq("saga.flight.response"), eq("TRV-003"), responseCaptor.capture());

        Map<String, Object> response = responseCaptor.getValue();
        assertFalse((Boolean) response.get("success"));
    }

    @Test
    @DisplayName("COMPENSATE: should cancel flight and restore seats")
    void shouldCancelFlight_andRestoreSeats() {
        // Given - first book a flight
        Map<String, Object> bookCommand = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-004",
                "processInstanceId", "proc-004",
                "travelerName", "Charlie",
                "destination", "Tokyo",
                "passengers", 5
        );
        flightConsumer.onCommand(bookCommand);

        // When - compensate (cancel)
        Map<String, Object> cancelCommand = Map.of(
                "action", "COMPENSATE",
                "bookingId", "TRV-004",
                "processInstanceId", "proc-004",
                "destination", "Tokyo"
        );
        flightConsumer.onCommand(cancelCommand);

        // Then - should send success response for cancellation
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate, times(2)).send(eq("saga.flight.response"), eq("TRV-004"), responseCaptor.capture());

        Map<String, Object> cancelResponse = responseCaptor.getAllValues().get(1);
        assertTrue((Boolean) cancelResponse.get("success"));
        assertTrue(((String) cancelResponse.get("message")).contains("cancelled"));
    }

    @Test
    @DisplayName("Seats should decrement after booking")
    void seatsShouldDecrement_afterBooking() {
        // Given - Tokyo has 80 seats, book 80
        for (int i = 0; i < 80; i++) {
            Map<String, Object> command = Map.of(
                    "action", "EXECUTE",
                    "bookingId", "TRV-BULK-" + i,
                    "processInstanceId", "proc-bulk-" + i,
                    "travelerName", "Bulk",
                    "destination", "Tokyo",
                    "passengers", 1
            );
            flightConsumer.onCommand(command);
        }

        // When - try to book one more
        Map<String, Object> lastCommand = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-LAST",
                "processInstanceId", "proc-last",
                "travelerName", "LastOne",
                "destination", "Tokyo",
                "passengers", 1
        );
        flightConsumer.onCommand(lastCommand);

        // Then - should fail (0 seats remaining)
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("saga.flight.response"), eq("TRV-LAST"), responseCaptor.capture());

        Map<String, Object> lastResponse = responseCaptor.getValue();
        assertFalse((Boolean) lastResponse.get("success"));
    }
}
