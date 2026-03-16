package io.orqueio.example.hotel.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class HotelConsumerTest {

    private HotelConsumer hotelConsumer;
    private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        hotelConsumer = new HotelConsumer(kafkaTemplate);
    }

    @Test
    @DisplayName("EXECUTE: should book hotel when budget is sufficient")
    void shouldBookHotel_whenBudgetSufficient() {
        // Given - Paris rate is 180 EUR/night, budget is 2000
        Map<String, Object> command = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-001",
                "processInstanceId", "proc-001",
                "travelerName", "Alice",
                "destination", "Paris",
                "budget", 2000.0
        );

        // When
        hotelConsumer.onCommand(command);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate).send(eq("saga.hotel.response"), eq("TRV-001"), responseCaptor.capture());

        Map<String, Object> response = responseCaptor.getValue();
        assertTrue((Boolean) response.get("success"));
        assertTrue(((String) response.get("message")).contains("Hotel booked in Paris"));
        assertEquals("hotel-service", response.get("serviceName"));
    }

    @Test
    @DisplayName("EXECUTE: should reject hotel when budget too low")
    void shouldRejectHotel_whenBudgetTooLow() {
        // Given - Dubai rate is 450 EUR/night, budget is 100
        Map<String, Object> command = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-002",
                "processInstanceId", "proc-002",
                "travelerName", "Charlie",
                "destination", "Dubai",
                "budget", 100.0
        );

        // When
        hotelConsumer.onCommand(command);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate).send(eq("saga.hotel.response"), eq("TRV-002"), responseCaptor.capture());

        Map<String, Object> response = responseCaptor.getValue();
        assertFalse((Boolean) response.get("success"));
        assertTrue(((String) response.get("message")).contains("insufficient"));
    }

    @Test
    @DisplayName("EXECUTE: should reject when no rooms available")
    void shouldRejectHotel_whenNoRoomsAvailable() {
        // Given - Dubai has 10 rooms, book all of them
        for (int i = 0; i < 10; i++) {
            Map<String, Object> command = Map.of(
                    "action", "EXECUTE",
                    "bookingId", "TRV-FILL-" + i,
                    "processInstanceId", "proc-fill-" + i,
                    "travelerName", "Guest " + i,
                    "destination", "Dubai",
                    "budget", 5000.0
            );
            hotelConsumer.onCommand(command);
        }

        // When - try one more
        Map<String, Object> lastCommand = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-FULL",
                "processInstanceId", "proc-full",
                "travelerName", "TooLate",
                "destination", "Dubai",
                "budget", 5000.0
        );
        hotelConsumer.onCommand(lastCommand);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("saga.hotel.response"), eq("TRV-FULL"), responseCaptor.capture());

        Map<String, Object> lastResponse = responseCaptor.getValue();
        assertFalse((Boolean) lastResponse.get("success"));
        assertTrue(((String) lastResponse.get("message")).contains("No rooms"));
    }

    @Test
    @DisplayName("COMPENSATE: should cancel hotel and restore room")
    void shouldCancelHotel_andRestoreRoom() {
        // Given - book a hotel first
        Map<String, Object> bookCommand = Map.of(
                "action", "EXECUTE",
                "bookingId", "TRV-003",
                "processInstanceId", "proc-003",
                "travelerName", "Eve",
                "destination", "London",
                "budget", 3000.0
        );
        hotelConsumer.onCommand(bookCommand);

        // When - compensate
        Map<String, Object> cancelCommand = Map.of(
                "action", "COMPENSATE",
                "bookingId", "TRV-003",
                "processInstanceId", "proc-003",
                "destination", "London"
        );
        hotelConsumer.onCommand(cancelCommand);

        // Then
        ArgumentCaptor<Map<String, Object>> responseCaptor = ArgumentCaptor.forClass(Map.class);
        verify(kafkaTemplate, times(2)).send(eq("saga.hotel.response"), eq("TRV-003"), responseCaptor.capture());

        Map<String, Object> cancelResponse = responseCaptor.getAllValues().get(1);
        assertTrue((Boolean) cancelResponse.get("success"));
        assertTrue(((String) cancelResponse.get("message")).contains("cancelled"));
    }
}
