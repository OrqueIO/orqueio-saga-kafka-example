package io.orqueio.example.travel;

import io.orqueio.bpm.engine.RuntimeService;
import io.orqueio.bpm.engine.runtime.ProcessInstance;
import io.orqueio.bpm.engine.test.Deployment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "saga.flight.command", "saga.flight.response",
                "saga.hotel.command", "saga.hotel.response",
                "saga.car.command", "saga.car.response"
        }
)
class TravelSagaProcessTest {

    @Autowired
    private RuntimeService runtimeService;

    @Test
    @DisplayName("Saga process should start and send flight command")
    void shouldStartSagaProcess() {
        // Given
        Map<String, Object> variables = new HashMap<>();
        variables.put("bookingId", "TEST-001");
        variables.put("travelerName", "Alice");
        variables.put("destination", "Paris");
        variables.put("departureDate", "2026-07-15");
        variables.put("returnDate", "2026-07-22");
        variables.put("passengers", 2);
        variables.put("budget", 2000.0);

        // When - start the process
        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-001", variables);

        // Then - process should be started and waiting at FlightResponse message catch event
        assertNotNull(processInstance);
        assertFalse(processInstance.isEnded());
    }

    @Test
    @DisplayName("Happy path: all services succeed → process completes")
    void happyPath_allServicesSucceed() {
        // Given
        Map<String, Object> variables = new HashMap<>();
        variables.put("bookingId", "TEST-HAPPY");
        variables.put("travelerName", "Bob");
        variables.put("destination", "Tokyo");
        variables.put("departureDate", "2026-08-01");
        variables.put("returnDate", "2026-08-07");
        variables.put("passengers", 1);
        variables.put("budget", 3000.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-HAPPY", variables);
        String processId = processInstance.getId();

        // When - simulate FlightResponse success
        runtimeService.createMessageCorrelation("FlightResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", true)
                .setVariable("serviceMessage", "Flight FL-TEST booked")
                .correlate();

        // Then - process should be waiting for HotelResponse
        assertNotNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult());

        // When - simulate HotelResponse success
        runtimeService.createMessageCorrelation("HotelResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", true)
                .setVariable("serviceMessage", "Hotel HTL-TEST booked")
                .correlate();

        // Then - process should be waiting for CarRentalResponse
        assertNotNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult());

        // When - simulate CarRentalResponse success
        runtimeService.createMessageCorrelation("CarRentalResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", true)
                .setVariable("serviceMessage", "Car CAR-TEST rented")
                .correlate();

        // Then - process should be completed (End_Success)
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should be completed after all services succeed");
    }

    @Test
    @DisplayName("Flight failure: process ends immediately without compensation")
    void flightFailure_noCompensation() {
        // Given
        Map<String, Object> variables = new HashMap<>();
        variables.put("bookingId", "TEST-FLIGHT-FAIL");
        variables.put("travelerName", "Diana");
        variables.put("destination", "Mars");
        variables.put("departureDate", "2026-10-01");
        variables.put("returnDate", "2026-10-07");
        variables.put("passengers", 1);
        variables.put("budget", 5000.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-FLIGHT-FAIL", variables);
        String processId = processInstance.getId();

        // When - simulate FlightResponse failure
        runtimeService.createMessageCorrelation("FlightResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", false)
                .setVariable("serviceMessage", "No flights to Mars")
                .correlate();

        // Then - process should be completed immediately (End_FlightFailed)
        // No compensation needed since nothing was booked before
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should end immediately when flight fails");
    }

    @Test
    @DisplayName("Hotel failure: flight gets compensated")
    void hotelFailure_flightCompensated() {
        // Given
        Map<String, Object> variables = new HashMap<>();
        variables.put("bookingId", "TEST-HOTEL-FAIL");
        variables.put("travelerName", "Charlie");
        variables.put("destination", "Dubai");
        variables.put("departureDate", "2026-09-01");
        variables.put("returnDate", "2026-09-05");
        variables.put("passengers", 1);
        variables.put("budget", 100.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-HOTEL-FAIL", variables);
        String processId = processInstance.getId();

        // When - simulate FlightResponse success
        runtimeService.createMessageCorrelation("FlightResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", true)
                .setVariable("serviceMessage", "Flight booked to Dubai")
                .correlate();

        // When - simulate HotelResponse failure
        runtimeService.createMessageCorrelation("HotelResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", false)
                .setVariable("serviceMessage", "Budget 100 EUR insufficient for Dubai (450 EUR/night)")
                .correlate();

        // Then - CancelFlight compensation runs, then process ends (End_HotelFailed)
        // The CancelFlightDelegate sends a Kafka command but process continues to end
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should complete after hotel fails and flight is compensated");
    }

    @Test
    @DisplayName("Car rental failure: hotel and flight get compensated in reverse order")
    void carFailure_hotelAndFlightCompensated() {
        // Given
        Map<String, Object> variables = new HashMap<>();
        variables.put("bookingId", "TEST-CAR-FAIL");
        variables.put("travelerName", "Eve");
        variables.put("destination", "Paris");
        variables.put("departureDate", "2026-11-01");
        variables.put("returnDate", "2026-11-07");
        variables.put("passengers", 1);
        variables.put("budget", 2000.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-CAR-FAIL", variables);
        String processId = processInstance.getId();

        // Flight success
        runtimeService.createMessageCorrelation("FlightResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", true)
                .setVariable("serviceMessage", "Flight booked")
                .correlate();

        // Hotel success
        runtimeService.createMessageCorrelation("HotelResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", true)
                .setVariable("serviceMessage", "Hotel booked")
                .correlate();

        // Car rental failure
        runtimeService.createMessageCorrelation("CarRentalResponse")
                .processInstanceId(processId)
                .setVariable("serviceSuccess", false)
                .setVariable("serviceMessage", "No cars available")
                .correlate();

        // Then - CancelHotel then CancelFlight compensations run, process ends (End_CarFailed)
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should complete after car fails and hotel+flight are compensated");
    }
}
