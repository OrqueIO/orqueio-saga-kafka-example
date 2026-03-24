package io.orqueio.example.hotel.kafka;

import io.orqueio.example.hotel.model.WorkerMessage;
import io.orqueio.example.hotel.model.WorkerResponse;
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

class HotelConsumerTest {

    private HotelConsumer hotelConsumer;
    private KafkaTemplate<String, WorkerResponse> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        hotelConsumer = new HotelConsumer(kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("EXECUTE: should book hotel when budget is sufficient")
    void shouldBookHotel_whenBudgetSufficient() throws Exception {
        // Given - Paris rate is 180 EUR/night, budget is 2000
        WorkerMessage command = new WorkerMessage("proc-001", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-001",
                        "travelerName", "Alice",
                        "destination", "Paris",
                        "budget", 2000.0
                )));

        // When
        hotelConsumer.onCommand(command);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate).send(eq("saga.hotel.response"), eq("TRV-001"), responseCaptor.capture());

        WorkerResponse response = responseCaptor.getValue();
        assertTrue(response.isSuccess());
        assertTrue(response.getPayload().contains("Hotel booked in Paris"));
    }

    @Test
    @DisplayName("EXECUTE: should reject hotel when budget too low")
    void shouldRejectHotel_whenBudgetTooLow() throws Exception {
        // Given - Dubai rate is 450 EUR/night, budget is 100
        WorkerMessage command = new WorkerMessage("proc-002", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-002",
                        "travelerName", "Charlie",
                        "destination", "Dubai",
                        "budget", 100.0
                )));

        // When
        hotelConsumer.onCommand(command);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate).send(eq("saga.hotel.response"), eq("TRV-002"), responseCaptor.capture());

        WorkerResponse response = responseCaptor.getValue();
        assertFalse(response.isSuccess());
        assertTrue(response.getPayload().contains("insufficient"));
    }

    @Test
    @DisplayName("EXECUTE: should reject when no rooms available")
    void shouldRejectHotel_whenNoRoomsAvailable() throws Exception {
        // Given - Dubai has 10 rooms, book all of them
        for (int i = 0; i < 10; i++) {
            WorkerMessage command = new WorkerMessage("proc-fill-" + i, "EXECUTE",
                    objectMapper.writeValueAsString(Map.of(
                            "bookingId", "TRV-FILL-" + i,
                            "travelerName", "Guest " + i,
                            "destination", "Dubai",
                            "budget", 5000.0
                    )));
            hotelConsumer.onCommand(command);
        }

        // When - try one more
        WorkerMessage lastCommand = new WorkerMessage("proc-full", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-FULL",
                        "travelerName", "TooLate",
                        "destination", "Dubai",
                        "budget", 5000.0
                )));
        hotelConsumer.onCommand(lastCommand);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("saga.hotel.response"), eq("TRV-FULL"), responseCaptor.capture());

        WorkerResponse lastResponse = responseCaptor.getValue();
        assertFalse(lastResponse.isSuccess());
        assertTrue(lastResponse.getPayload().contains("No rooms"));
    }

    @Test
    @DisplayName("COMPENSATE: should cancel hotel and restore room")
    void shouldCancelHotel_andRestoreRoom() throws Exception {
        // Given - book a hotel first
        WorkerMessage bookCommand = new WorkerMessage("proc-003", "EXECUTE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-003",
                        "travelerName", "Eve",
                        "destination", "London",
                        "budget", 3000.0
                )));
        hotelConsumer.onCommand(bookCommand);

        // When - compensate
        WorkerMessage cancelCommand = new WorkerMessage("proc-003", "COMPENSATE",
                objectMapper.writeValueAsString(Map.of(
                        "bookingId", "TRV-003",
                        "destination", "London"
                )));
        hotelConsumer.onCommand(cancelCommand);

        // Then
        ArgumentCaptor<WorkerResponse> responseCaptor = ArgumentCaptor.forClass(WorkerResponse.class);
        verify(kafkaTemplate, times(2)).send(eq("saga.hotel.response"), eq("TRV-003"), responseCaptor.capture());

        WorkerResponse cancelResponse = responseCaptor.getAllValues().get(1);
        assertTrue(cancelResponse.isSuccess());
        assertTrue(cancelResponse.getPayload().contains("cancelled"));
    }
}
