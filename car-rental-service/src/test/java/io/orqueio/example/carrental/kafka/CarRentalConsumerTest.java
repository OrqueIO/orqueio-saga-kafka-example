package io.orqueio.example.carrental.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class CarRentalConsumerTest {

    private CarRentalConsumer carRentalConsumer;
    private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        carRentalConsumer = new CarRentalConsumer(kafkaTemplate);
    }

    @Test
    @DisplayName("EXECUTE: should rent car when cars are available")
    void shouldRentCar_whenAvailable() {
        // Given
        Map<String, Object> command = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-001",
                "processInstanceId", "proc-001",
                "travelerName", "Alice",
                "destination", "Paris",
                "departureDate", "2026-07-15",
                "returnDate", "2026-07-22"
        );

        // When
        carRentalConsumer.onCommand(command);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate).send(eq("saga.car.response"), eq("TRV-001"), responseCaptor.capture());

        Map<String, Object> response = responseCaptor.getValue();
        assertTrue((Boolean) response.get("success"));
        assertEquals("car-rental-service", response.get("serviceName"));
    }

    @Test
    @DisplayName("EXECUTE: should reject when no cars available for destination")
    void shouldRejectCar_whenNoneAvailable() {
        // Given - "Mars" has no cars
        Map<String, Object> command = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-002",
                "processInstanceId", "proc-002",
                "travelerName", "Diana",
                "destination", "Mars",
                "departureDate", "2026-10-01",
                "returnDate", "2026-10-07"
        );

        // When
        carRentalConsumer.onCommand(command);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate).send(eq("saga.car.response"), eq("TRV-002"), responseCaptor.capture());

        Map<String, Object> response = responseCaptor.getValue();
        assertFalse((Boolean) response.get("success"));
    }

    @Test
    @DisplayName("COMPENSATE: should cancel rental and restore car")
    void shouldCancelRental_andRestoreCar() {
        // Given - first rent a car
        Map<String, Object> rentCommand = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-003",
                "processInstanceId", "proc-003",
                "travelerName", "Bob",
                "destination", "Tokyo",
                "departureDate", "2026-08-01",
                "returnDate", "2026-08-07"
        );
        carRentalConsumer.onCommand(rentCommand);

        // When - compensate
        Map<String, Object> cancelCommand = Map.of(
                "action", "COMPENSATE",
                "bookingId", "TRV-003",
                "processInstanceId", "proc-003",
                "destination", "Tokyo"
        );
        carRentalConsumer.onCommand(cancelCommand);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate, times(2)).send(eq("saga.car.response"), eq("TRV-003"), responseCaptor.capture());

        Map<String, Object> cancelResponse = responseCaptor.getAllValues().get(1);
        assertTrue((Boolean) cancelResponse.get("success"));
        assertTrue(((String) cancelResponse.get("message")).contains("cancelled"));
    }
}
