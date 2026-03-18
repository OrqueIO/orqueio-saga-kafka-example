package io.orqueio.example.travel;

import io.orqueio.bpm.engine.RuntimeService;
import io.orqueio.bpm.engine.runtime.ProcessInstance;
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

    private Map<String, Object> createVariables(String bookingId, String travelerName,
                                                 String destination, String departureDate,
                                                 String returnDate, int passengers, double budget) {
        Map<String, Object> variables = new HashMap<>();
        variables.put("bookingId", bookingId);
        variables.put("travelerName", travelerName);
        variables.put("destination", destination);
        variables.put("departureDate", departureDate);
        variables.put("returnDate", returnDate);
        variables.put("passengers", passengers);
        variables.put("budget", budget);
        return variables;
    }

    /**
     * Find the callworker subprocess instance spawned by a parent process.
     * The message catch events live in the subprocess, so correlation must target it.
     */
    private String findCallWorkerSubProcessId(String parentProcessId) {
        ProcessInstance subProcess = runtimeService.createProcessInstanceQuery()
                .superProcessInstanceId(parentProcessId)
                .singleResult();
        assertNotNull(subProcess, "Call Worker subprocess should be active");
        return subProcess.getId();
    }

    @Test
    @DisplayName("Saga process should start and send flight command via Call Worker")
    void shouldStartSagaProcess() {
        // Given
        Map<String, Object> variables = createVariables(
                "TEST-001", "Alice", "Paris", "2026-07-15", "2026-07-22", 2, 2000.0);

        // When - start the process
        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-001", variables);

        // Then - process should be started and waiting inside the callworker subprocess
        assertNotNull(processInstance);
        assertFalse(processInstance.isEnded());

        // A callworker subprocess should be active
        String subProcessId = findCallWorkerSubProcessId(processInstance.getId());
        assertNotNull(subProcessId);
    }

    @Test
    @DisplayName("Happy path: all services succeed → process completes")
    void happyPath_allServicesSucceed() {
        // Given
        Map<String, Object> variables = createVariables(
                "TEST-HAPPY", "Bob", "Tokyo", "2026-08-01", "2026-08-07", 1, 3000.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-HAPPY", variables);
        String processId = processInstance.getId();

        // Flight OK → correlate to the callworker subprocess
        correlateOkToSubProcess(processId);

        // Then - process should still be running (waiting for hotel in next callworker)
        assertNotNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult());

        // Hotel OK
        correlateOkToSubProcess(processId);

        // Then - process should still be running (waiting for car)
        assertNotNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult());

        // Car OK
        correlateOkToSubProcess(processId);

        // Then - process should be completed (End_Success)
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should be completed after all services succeed");
    }

    @Test
    @DisplayName("Flight failure: process ends immediately without compensation")
    void flightFailure_noCompensation() {
        // Given
        Map<String, Object> variables = createVariables(
                "TEST-FLIGHT-FAIL", "Diana", "Mars", "2026-10-01", "2026-10-07", 1, 5000.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-FLIGHT-FAIL", variables);
        String processId = processInstance.getId();

        // Flight KO → boundary error catches CALL_KO, process ends at End_FlightFailed
        correlateKoToSubProcess(processId);

        // Then - process should be completed immediately
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should end immediately when flight fails");
    }

    @Test
    @DisplayName("Hotel failure: flight gets compensated")
    void hotelFailure_flightCompensated() {
        // Given
        Map<String, Object> variables = createVariables(
                "TEST-HOTEL-FAIL", "Charlie", "Dubai", "2026-09-01", "2026-09-05", 1, 100.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-HOTEL-FAIL", variables);
        String processId = processInstance.getId();

        // Flight success
        correlateOkToSubProcess(processId);

        // Hotel failure → boundary error catches CALL_KO → CancelFlight compensation runs
        correlateKoToSubProcess(processId);

        // Then - process ends (End_HotelFailed)
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should complete after hotel fails and flight is compensated");
    }

    @Test
    @DisplayName("Car rental failure: hotel and flight get compensated in reverse order")
    void carFailure_hotelAndFlightCompensated() {
        // Given
        Map<String, Object> variables = createVariables(
                "TEST-CAR-FAIL", "Eve", "Paris", "2026-11-01", "2026-11-07", 1, 2000.0);

        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(
                "travel-saga", "TEST-CAR-FAIL", variables);
        String processId = processInstance.getId();

        // Flight success
        correlateOkToSubProcess(processId);

        // Hotel success
        correlateOkToSubProcess(processId);

        // Car failure → CancelHotel then CancelFlight compensations
        correlateKoToSubProcess(processId);

        // Then - process ends (End_CarFailed)
        assertNull(runtimeService.createProcessInstanceQuery()
                .processInstanceId(processId).singleResult(),
                "Process should complete after car fails and hotel+flight are compensated");
    }

    private void correlateOkToSubProcess(String parentProcessId) {
        String subProcessId = findCallWorkerSubProcessId(parentProcessId);
        runtimeService.createMessageCorrelation("return-call-ok-message")
                .processInstanceId(subProcessId)
                .setVariable("serviceSuccess", true)
                .setVariable("serviceMessage", "Service completed successfully")
                .correlate();
    }

    private void correlateKoToSubProcess(String parentProcessId) {
        String subProcessId = findCallWorkerSubProcessId(parentProcessId);
        runtimeService.createMessageCorrelation("return-call-ko-message")
                .processInstanceId(subProcessId)
                .setVariable("serviceSuccess", false)
                .setVariable("serviceMessage", "Service failed")
                .correlate();
    }
}
