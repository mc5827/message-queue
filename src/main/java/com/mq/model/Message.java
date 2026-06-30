package com.mq.model;

import java.util.UUID;

public class Message {

    private final String id;
    private final Object payload;
    private MessageState state;
    private final long enqueuedAt;
    private int retryCount;
    private long visibilityDeadline;

    public Message(Object payload) {
        this.id = UUID.randomUUID().toString();
        this.payload = payload;
        this.state = MessageState.QUEUED;
        this.enqueuedAt = System.currentTimeMillis();
        this.retryCount = 0;
        this.visibilityDeadline = 0;
    }

    public String getId() { return id; }
    public Object getPayload() { return payload; }
    public MessageState getState() { return state; }
    public long getEnqueuedAt() { return enqueuedAt; }
    public int getRetryCount() { return retryCount; }
    public long getVisibilityDeadline() { return visibilityDeadline; }

    public void setState(MessageState state) { this.state = state; }
    public void setVisibilityDeadline(long visibilityDeadline) { this.visibilityDeadline = visibilityDeadline; }
    public void incrementRetryCount() { this.retryCount++; }
    public void resetRetryCount() { this.retryCount = 0; }
}