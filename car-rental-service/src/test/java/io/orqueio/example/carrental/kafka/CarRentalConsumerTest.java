package io.orqueio.example.carrental.kafka;

import io.orqueio.example.carrental.model.WorkerMessage;
import io.orqueio.example.carrental.model.WorkerResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class CarRentalConsumerTest {

    private CarRentalConsumer carRentalConsumer;
    private KafkaTemplate<String, WorkerResponse> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        carRentalConsumer = new CarRentalConsumer(kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("EXECUTE: should rent car when cars are available")
    void shouldRentCar_whenAvailable() throws Exception {
        // Given
        WorkerMessage command = new WorkerMessage("proc-001", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-001",
                        "travelerName", "Alice",
                        "destination", "Paris",
                        "departureDate", "2026-07-15",
                        "returnDate", "2026-07-22"
                )));

        // When
        carRentalConsumer.onCommand(command);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate).send(eq("saga.car.response"), eq("TRV-001"), responseCaptor.capture());

        WorkerResponse response = responseCaptor.getValue();
        assertTrue(response.isSuccess());
    }

    @Test
    @DisplayName("EXECUTE: should reject when no cars available for destination")
    void shouldRejectCar_whenNoneAvailable() throws Exception {
        // Given - "Mars" has no cars
        WorkerMessage command = new WorkerMessage("proc-002", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-002",
                        "travelerName", "Diana",
                        "destination", "Mars",
                        "departureDate", "2026-10-01",
                        "returnDate", "2026-10-07"
                )));

        // When
        carRentalConsumer.onCommand(command);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate).send(eq("saga.car.response"), eq("TRV-002"), responseCaptor.capture());

        assertFalse(responseCaptor.getValue().isSuccess());
    }

    @Test
    @DisplayName("COMPENSATE: should cancel rental and restore car")
    void shouldCancelRental_andRestoreCar() throws Exception {
        // Given - first rent a car
        WorkerMessage rentCommand = new WorkerMessage("proc-003", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-003",
                        "travelerName", "Bob",
                        "destination", "Tokyo",
                        "departureDate", "2026-08-01",
                        "returnDate", "2026-08-07"
                )));
        carRentalConsumer.onCommand(rentCommand);

        // When - compensate
        WorkerMessage cancelCommand = new WorkerMessage("proc-003", "COMPENSATE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-003",
                        "destination", "Tokyo"
                )));
        carRentalConsumer.onCommand(cancelCommand);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate, times(2)).send(eq("saga.car.response"), eq("TRV-003"), responseCaptor.capture());

        WorkerResponse cancelResponse = responseCaptor.getAllValues().get(1);
        assertTrue(cancelResponse.isSuccess());
        assertTrue(cancelResponse.getPayload().contains("cancelled"));
    }
}
