package io.orqueio.example.travel.kafka;

import io.orqueio.bpm.engine.RuntimeService;
import io.orqueio.example.travel.config.KafkaTopics;
import io.orqueio.example.travel.model.SagaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class SagaResponseConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaResponseConsumer.class);
    private final RuntimeService runtimeService;

    public SagaResponseConsumer(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @KafkaListener(topics = KafkaTopics.FLIGHT_RESPONSE, groupId = "saga-orchestrator")
    public void onFlightResponse(SagaResponse response) {
        log.info("[SAGA] Flight response for booking {}: success={}, message={}",
                response.getBookingId(), response.isSuccess(), response.getMessage());
        correlateMessage("FlightResponse", response);
    }

    @KafkaListener(topics = KafkaTopics.HOTEL_RESPONSE, groupId = "saga-orchestrator")
    public void onHotelResponse(SagaResponse response) {
        log.info("[SAGA] Hotel response for booking {}: success={}, message={}",
                response.getBookingId(), response.isSuccess(), response.getMessage());
        correlateMessage("HotelResponse", response);
    }

    @KafkaListener(topics = KafkaTopics.CAR_RESPONSE, groupId = "saga-orchestrator")
    public void onCarResponse(SagaResponse response) {
        log.info("[SAGA] Car rental response for booking {}: success={}, message={}",
                response.getBookingId(), response.isSuccess(), response.getMessage());
        correlateMessage("CarRentalResponse", response);
    }

    private void correlateMessage(String messageName, SagaResponse response) {
        Map<String, Object> variables = new HashMap<>();
        variables.put("serviceSuccess", response.isSuccess());
        variables.put("serviceMessage", response.getMessage());

        runtimeService.createMessageCorrelation(messageName)
                .processInstanceId(response.getProcessInstanceId())
                .setVariables(variables)
                .correlate();
    }
}
