package io.orqueio.example.travel.delegate;

import io.orqueio.bpm.engine.delegate.DelegateExecution;
import io.orqueio.bpm.engine.delegate.JavaDelegate;
import io.orqueio.example.travel.config.KafkaTopics;
import io.orqueio.example.travel.model.SagaCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component("rentCar")
public class RentCarDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(RentCarDelegate.class);
    private final KafkaTemplate<String, SagaCommand> kafkaTemplate;

    public RentCarDelegate(KafkaTemplate<String, SagaCommand> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String bookingId = (String) execution.getVariable("bookingId");
        String travelerName = (String) execution.getVariable("travelerName");
        String destination = (String) execution.getVariable("destination");
        String departureDate = (String) execution.getVariable("departureDate");
        String returnDate = (String) execution.getVariable("returnDate");

        SagaCommand command = new SagaCommand(bookingId, execution.getProcessInstanceId(), "EXECUTE");
        command.setTravelerName(travelerName);
        command.setDestination(destination);
        command.setDepartureDate(departureDate);
        command.setReturnDate(returnDate);

        log.info("[SAGA] Sending car rental command for {} in {}", travelerName, destination);
        kafkaTemplate.send(KafkaTopics.CAR_COMMAND, bookingId, command);
    }
}
