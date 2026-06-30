package com.mq.dlq;

import com.mq.model.Message;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DeadLetterQueue implements IDeadLetterQueue {

    private final Queue<Message> dlqStore;

    public DeadLetterQueue() {
        this.dlqStore = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void add(Message message) {
        dlqStore.offer(message);
    }

    @Override
    public Message poll() {
        return dlqStore.poll();
    }

    @Override
    public List<Message> getMessages() {
        return List.copyOf(dlqStore);
    }
}
