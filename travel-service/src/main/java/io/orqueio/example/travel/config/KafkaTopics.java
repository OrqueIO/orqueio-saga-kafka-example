package io.orqueio.example.travel.config;

public final class KafkaTopics {

    private KafkaTopics() {}

    // Flight
    public static final String FLIGHT_COMMAND = "saga.flight.command";
    public static final String FLIGHT_RESPONSE = "saga.flight.response";

    // Hotel
    public static final String HOTEL_COMMAND = "saga.hotel.command";
    public static final String HOTEL_RESPONSE = "saga.hotel.response";

    // Car Rental
    public static final String CAR_COMMAND = "saga.car.command";
    public static final String CAR_RESPONSE = "saga.car.response";
}
