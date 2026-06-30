package com.mq.queue;

import com.mq.dlq.IDeadLetterQueue;
import com.mq.model.Message;
import com.mq.model.MessageState;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageQueue implements IMessageQueue {

    private final int maxRetries;
    private final long backoffBaseMs;
    private final IDeadLetterQueue dlq;
    private final Queue<String> mainQueue;
    private final Map<String, Message> messageStore;
    private final ScheduledExecutorService scheduler;

    public MessageQueue(int maxRetries, IDeadLetterQueue dlq) {
        this(maxRetries, dlq, 1000);
    }

    public MessageQueue(int maxRetries, IDeadLetterQueue dlq, long backoffBaseMs) {
        this.maxRetries = maxRetries;
        this.backoffBaseMs = backoffBaseMs;
        this.dlq = dlq;
        this.mainQueue = new ArrayDeque<>();
        this.messageStore = new HashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public synchronized String publish(Object payload) {
        Message message = new Message(payload);
        messageStore.put(message.getId(), message);
        mainQueue.offer(message.getId());
        return message.getId();
    }

    @Override
    public synchronized Message consume(long visibilityTimeoutMs) {
        String messageId = mainQueue.poll();
        if (messageId == null) {
            return null;
        }
        Message message = messageStore.get(messageId);
        message.setState(MessageState.IN_FLIGHT);
        message.setVisibilityDeadline(System.currentTimeMillis() + visibilityTimeoutMs);
        scheduler.schedule(() -> handleTimeout(messageId), visibilityTimeoutMs, TimeUnit.MILLISECONDS);
        return message;
    }

    @Override
    public synchronized void ack(String messageId) {
        Message message = messageStore.get(messageId);
        if (message == null) {
            throw new IllegalArgumentException("Unknown messageId: " + messageId);
        }
        if (message.getState() != MessageState.IN_FLIGHT) {
            throw new IllegalStateException("Message is not IN_FLIGHT, current state: " + message.getState());
        }
        messageStore.remove(messageId);
    }

    @Override
    public synchronized void requeue(Message message) {
        message.resetRetryCount();
        message.setState(MessageState.QUEUED);
        message.setVisibilityDeadline(0);
        messageStore.put(message.getId(), message);
        mainQueue.offer(message.getId());
    }

    @Override
    public synchronized void nack(String messageId) {
        Message message = messageStore.get(messageId);
        if (message == null) {
            throw new IllegalArgumentException("Unknown messageId: " + messageId);
        }
        if (message.getState() != MessageState.IN_FLIGHT) {
            throw new IllegalStateException("Message is not IN_FLIGHT, current state: " + message.getState());
        }
        requeueOrDead(message);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    private synchronized void handleTimeout(String messageId) {
        Message message = messageStore.get(messageId);
        if (message == null) {
            return; // already ACKed
        }
        if (message.getState() == MessageState.QUEUED) {
            return; // nack() already handled it
        }
        if (message.getState() == MessageState.DEAD) {
            throw new IllegalStateException("Message in unexpected DEAD state during timeout: " + messageId);
        }
        requeueOrDead(message);
    }

    private void requeueOrDead(Message message) {
        message.incrementRetryCount();
        message.setVisibilityDeadline(0);
        if (message.getRetryCount() >= maxRetries) {
            messageStore.remove(message.getId());
            message.setState(MessageState.DEAD);
            dlq.add(message);
        } else {
            message.setState(MessageState.QUEUED);
            long delayMs = calculateBackoff(message.getRetryCount());
            scheduler.schedule(() -> {
                synchronized (this) {
                    mainQueue.offer(message.getId());
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private long calculateBackoff(int retryCount) {
        long exponentialMs = (long) Math.pow(2, retryCount) * backoffBaseMs;
        long jitterMs = (long) (Math.random() * backoffBaseMs);
        return exponentialMs + jitterMs;
    }
}
