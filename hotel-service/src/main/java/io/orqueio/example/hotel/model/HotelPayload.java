package io.orqueio.example.hotel.model;

public class HotelPayload {

    private String bookingId;
    private String travelerName;
    private String destination;
    private double budget;

    public HotelPayload() {}

    public String getBookingId() { return bookingId; }
    public void setBookingId(String bookingId) { this.bookingId = bookingId; }

    public String getTravelerName() { return travelerName; }
    public void setTravelerName(String travelerName) { this.travelerName = travelerName; }

    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }

    public double getBudget() { return budget; }
    public void setBudget(double budget) { this.budget = budget; }
}
