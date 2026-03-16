package io.orqueio.example.travel.model;

public class SagaCommand {

    private String bookingId;
    private String processInstanceId;
    private String action; // EXECUTE or COMPENSATE
    private String travelerName;
    private String destination;
    private String departureDate;
    private String returnDate;
    private int passengers;
    private double budget;

    public SagaCommand() {}

    public SagaCommand(String bookingId, String processInstanceId, String action) {
        this.bookingId = bookingId;
        this.processInstanceId = processInstanceId;
        this.action = action;
    }

    public String getBookingId() { return bookingId; }
    public void setBookingId(String bookingId) { this.bookingId = bookingId; }

    public String getProcessInstanceId() { return processInstanceId; }
    public void setProcessInstanceId(String processInstanceId) { this.processInstanceId = processInstanceId; }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public String getTravelerName() { return travelerName; }
    public void setTravelerName(String travelerName) { this.travelerName = travelerName; }

    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }

    public String getDepartureDate() { return departureDate; }
    public void setDepartureDate(String departureDate) { this.departureDate = departureDate; }

    public String getReturnDate() { return returnDate; }
    public void setReturnDate(String returnDate) { this.returnDate = returnDate; }

    public int getPassengers() { return passengers; }
    public void setPassengers(int passengers) { this.passengers = passengers; }

    public double getBudget() { return budget; }
    public void setBudget(double budget) { this.budget = budget; }
}
