package io.orqueio.example.travel.delegate;

import io.orqueio.bpm.engine.delegate.DelegateExecution;
import io.orqueio.bpm.engine.delegate.JavaDelegate;
import io.orqueio.example.travel.config.KafkaTopics;
import io.orqueio.example.travel.model.SagaCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component("cancelHotel")
public class CancelHotelDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(CancelHotelDelegate.class);
    private final KafkaTemplate<String, SagaCommand> kafkaTemplate;

    public CancelHotelDelegate(KafkaTemplate<String, SagaCommand> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String bookingId = (String) execution.getVariable("bookingId");

        SagaCommand command = new SagaCommand(bookingId, execution.getProcessInstanceId(), "COMPENSATE");
        command.setTravelerName((String) execution.getVariable("travelerName"));
        command.setDestination((String) execution.getVariable("destination"));

        log.info("[SAGA-COMPENSATE] Sending hotel cancellation for booking {}", bookingId);
        kafkaTemplate.send(KafkaTopics.HOTEL_COMMAND, bookingId, command);
    }
}
