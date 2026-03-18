package io.orqueio.example.flight.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.orqueio.example.flight.model.WorkerMessage;
import io.orqueio.example.flight.model.WorkerResponse;
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
    private KafkaTemplate<String, WorkerResponse> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        flightConsumer = new FlightConsumer(kafkaTemplate, objectMapper);
    }

    private WorkerMessage buildCommand(String action, Map<String, Object> payload) throws Exception {
        return new WorkerMessage("proc-001", action, objectMapper.writeValueAsString(payload));
    }

    @Test
    @DisplayName("EXECUTE: should book flight when seats are available")
    void shouldBookFlight_whenSeatsAvailable() throws Exception {
        // Given - Paris has 120 seats
        WorkerMessage command = buildCommand("EXECUTE", Map.of(
                "bookingId", "TRV-001",
                "travelerName", "Alice",
                "destination", "Paris",
                "passengers", 2
        ));

        // When
        flightConsumer.onCommand(command);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate).send(eq("saga.flight.response"), eq("TRV-001"), responseCaptor.capture());

        WorkerResponse response = responseCaptor.getValue();
        assertTrue(response.isSuccess());
        assertEquals("proc-001", response.getCorrelationId());
    }

    @Test
    @DisplayName("EXECUTE: should reject flight when destination unknown (0 seats)")
    void shouldRejectFlight_whenDestinationUnknown() throws Exception {
        // Given - "Mars" has 0 seats
        WorkerMessage command = new WorkerMessage("proc-002", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-002",
                        "travelerName", "Diana",
                        "destination", "Mars",
                        "passengers", 1
                )));

        // When
        flightConsumer.onCommand(command);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate).send(eq("saga.flight.response"), eq("TRV-002"), responseCaptor.capture());

        WorkerResponse response = responseCaptor.getValue();
        assertFalse(response.isSuccess());
        assertTrue(response.getPayload().contains("Mars"));
    }

    @Test
    @DisplayName("EXECUTE: should reject flight when not enough seats")
    void shouldRejectFlight_whenNotEnoughSeats() throws Exception {
        // Given - Sydney has 40 seats, requesting 50
        WorkerMessage command = new WorkerMessage("proc-003", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-003",
                        "travelerName", "Group",
                        "destination", "Sydney",
                        "passengers", 50
                )));

        // When
        flightConsumer.onCommand(command);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate).send(eq("saga.flight.response"), eq("TRV-003"), responseCaptor.capture());

        assertFalse(responseCaptor.getValue().isSuccess());
    }

    @Test
    @DisplayName("COMPENSATE: should cancel flight and restore seats")
    void shouldCancelFlight_andRestoreSeats() throws Exception {
        // Given - first book a flight
        WorkerMessage bookCommand = new WorkerMessage("proc-004", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-004",
                        "travelerName", "Charlie",
                        "destination", "Tokyo",
                        "passengers", 5
                )));
        flightConsumer.onCommand(bookCommand);

        // When - compensate (cancel)
        WorkerMessage cancelCommand = new WorkerMessage("proc-004", "COMPENSATE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-004",
                        "destination", "Tokyo"
                )));
        flightConsumer.onCommand(cancelCommand);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate, times(2)).send(eq("saga.flight.response"), eq("TRV-004"), responseCaptor.capture());

        WorkerResponse cancelResponse = responseCaptor.getAllValues().get(1);
        assertTrue(cancelResponse.isSuccess());
        assertTrue(cancelResponse.getPayload().contains("cancelled"));
    }

    @Test
    @DisplayName("Seats should decrement after booking")
    void seatsShouldDecrement_afterBooking() throws Exception {
        // Given - Tokyo has 80 seats, book 80
        for (int i = 0; i < 80; i++) {
            WorkerMessage command = new WorkerMessage("proc-bulk-" + i, "EXECUTE",
                    objectMapper.writeValueAsString(Map.of(
                            "bookingId", "TRV-BULK-" + i,
                            "travelerName", "Bulk",
                            "destination", "Tokyo",
                            "passengers", 1
                    )));
            flightConsumer.onCommand(command);
        }

        // When - try to book one more
        WorkerMessage lastCommand = new WorkerMessage("proc-last", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-LAST",
                        "travelerName", "LastOne",
                        "destination", "Tokyo",
                        "passengers", 1
                )));
        flightConsumer.onCommand(lastCommand);

        // Then - should fail (0 seats remaining)
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("saga.flight.response"), eq("TRV-LAST"), responseCaptor.capture());

        assertFalse(responseCaptor.getValue().isSuccess());
    }
}
