package io.orqueio.example.travel.model;

/**
 * Generic message used for all Kafka communication between the orchestrator and microservices.
 * No business-specific fields — the payload is a JSON string that each microservice
 * converts to its own domain objects.
 */
public class WorkerMessage {

    private String correlationId;
    private String action;
    private String payload;

    public WorkerMessage() {}

    public WorkerMessage(String correlationId, String action, String payload) {
        this.correlationId = correlationId;
        this.action = action;
        this.payload = payload;
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
