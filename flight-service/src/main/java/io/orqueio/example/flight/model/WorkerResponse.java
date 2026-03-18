package io.orqueio.example.flight.model;

public class WorkerResponse {

    private String correlationId;
    private boolean success;
    private String payload;

    public WorkerResponse() {}

    public WorkerResponse(String correlationId, boolean success, String payload) {
        this.correlationId = correlationId;
        this.success = success;
        this.payload = payload;
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
