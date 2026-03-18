package io.orqueio.example.travel.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.orqueio.bpm.engine.delegate.DelegateExecution;
import io.orqueio.bpm.engine.delegate.JavaDelegate;
import io.orqueio.example.travel.model.WorkerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic Kafka message producer used by the Call Worker subprocess.
 * Reads the command topic and action from process variables,
 * serializes all business variables as a JSON payload.
 */
@Component("messageProducer")
public class MessageProducerDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(MessageProducerDelegate.class);
    private final KafkaTemplate<String, WorkerMessage> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public MessageProducerDelegate(KafkaTemplate<String, WorkerMessage> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String commandTopic = (String) execution.getVariable("commandTopic");
        String action = (String) execution.getVariable("workerAction");
        String bookingId = (String) execution.getVariable("bookingId");

        // Build business payload from process variables
        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("bookingId", bookingId);
        payloadMap.put("travelerName", execution.getVariable("travelerName"));
        payloadMap.put("destination", execution.getVariable("destination"));
        payloadMap.put("departureDate", execution.getVariable("departureDate"));
        payloadMap.put("returnDate", execution.getVariable("returnDate"));
        payloadMap.put("passengers", execution.getVariable("passengers"));
        payloadMap.put("budget", execution.getVariable("budget"));

        String jsonPayload = objectMapper.writeValueAsString(payloadMap);

        WorkerMessage message = new WorkerMessage(
                execution.getProcessInstanceId(), action, jsonPayload);

        log.info("[CALL-WORKER] Sending {} to topic '{}' for booking {}", action, commandTopic, bookingId);
        kafkaTemplate.send(commandTopic, bookingId, message);
    }
}
