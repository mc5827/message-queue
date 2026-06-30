package com.mq.dlq;

import com.mq.model.Message;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class DeadLetterQueue implements IDeadLetterQueue {

    private final Queue<Message> dlqStore;

    public DeadLetterQueue() {
        this.dlqStore = new ArrayDeque<>();
    }

    @Override
    public synchronized void add(Message message) {
        dlqStore.offer(message);
    }

    @Override
    public synchronized Message poll() {
        return dlqStore.poll();
    }

    @Override
    public synchronized List<Message> getMessages() {
        return List.copyOf(dlqStore);
    }
}
