package io.orqueio.example.travel.kafka;

import io.orqueio.bpm.engine.RuntimeService;
import io.orqueio.example.travel.model.WorkerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic Kafka response consumer.
 * Listens to all response topics and correlates using a single message name
 * "return-call-ok-message" or "return-call-ko-message" matching the Call Worker BPMN.
 */
@Component
public class WorkerResponseConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkerResponseConsumer.class);
    private final RuntimeService runtimeService;

    public WorkerResponseConsumer(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @KafkaListener(
            topics = {"saga.flight.response", "saga.hotel.response", "saga.car.response"},
            groupId = "saga-orchestrator"
    )
    public void onResponse(WorkerResponse response) {
        log.info("[CALL-WORKER] Response received: correlationId={}, success={}, payload={}",
                response.getCorrelationId(), response.isSuccess(), response.getPayload());

        String messageName = response.isSuccess()
                ? "return-call-ok-message"
                : "return-call-ko-message";

        Map<String, Object> variables = new HashMap<>();
        variables.put("serviceSuccess", response.isSuccess());
        variables.put("serviceMessage", response.getPayload());

        runtimeService.createMessageCorrelation(messageName)
                .processInstanceId(response.getCorrelationId())
                .setVariables(variables)
                .correlate();
    }
}
