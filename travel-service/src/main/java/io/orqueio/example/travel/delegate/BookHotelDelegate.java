package io.orqueio.example.travel.delegate;

import io.orqueio.bpm.engine.delegate.DelegateExecution;
import io.orqueio.bpm.engine.delegate.JavaDelegate;
import io.orqueio.example.travel.config.KafkaTopics;
import io.orqueio.example.travel.model.SagaCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component("bookHotel")
public class BookHotelDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(BookHotelDelegate.class);
    private final KafkaTemplate<String, SagaCommand> kafkaTemplate;

    public BookHotelDelegate(KafkaTemplate<String, SagaCommand> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String bookingId = (String) execution.getVariable("bookingId");
        String travelerName = (String) execution.getVariable("travelerName");
        String destination = (String) execution.getVariable("destination");
        String departureDate = (String) execution.getVariable("departureDate");
        String returnDate = (String) execution.getVariable("returnDate");
        double budget = ((Number) execution.getVariable("budget")).doubleValue();

        SagaCommand command = new SagaCommand(bookingId, execution.getProcessInstanceId(), "EXECUTE");
        command.setTravelerName(travelerName);
        command.setDestination(destination);
        command.setDepartureDate(departureDate);
        command.setReturnDate(returnDate);
        command.setBudget(budget);

        log.info("[SAGA] Sending hotel booking command for {} in {}", travelerName, destination);
        kafkaTemplate.send(KafkaTopics.HOTEL_COMMAND, bookingId, command);
    }
}
