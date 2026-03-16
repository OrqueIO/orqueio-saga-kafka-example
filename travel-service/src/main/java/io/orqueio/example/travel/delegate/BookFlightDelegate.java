package io.orqueio.example.travel.delegate;

import io.orqueio.bpm.engine.delegate.DelegateExecution;
import io.orqueio.bpm.engine.delegate.JavaDelegate;
import io.orqueio.example.travel.config.KafkaTopics;
import io.orqueio.example.travel.model.SagaCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component("bookFlight")
public class BookFlightDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(BookFlightDelegate.class);
    private final KafkaTemplate<String, SagaCommand> kafkaTemplate;

    public BookFlightDelegate(KafkaTemplate<String, SagaCommand> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String bookingId = (String) execution.getVariable("bookingId");
        String travelerName = (String) execution.getVariable("travelerName");
        String destination = (String) execution.getVariable("destination");
        String departureDate = (String) execution.getVariable("departureDate");
        String returnDate = (String) execution.getVariable("returnDate");
        int passengers = ((Number) execution.getVariable("passengers")).intValue();

        SagaCommand command = new SagaCommand(bookingId, execution.getProcessInstanceId(), "EXECUTE");
        command.setTravelerName(travelerName);
        command.setDestination(destination);
        command.setDepartureDate(departureDate);
        command.setReturnDate(returnDate);
        command.setPassengers(passengers);

        log.info("[SAGA] Sending flight booking command for {} to {}", travelerName, destination);
        kafkaTemplate.send(KafkaTopics.FLIGHT_COMMAND, bookingId, command);
    }
}
