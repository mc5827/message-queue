package com.mq.broker;

import com.mq.dlq.DeadLetterQueue;
import com.mq.dlq.IDeadLetterQueue;
import com.mq.model.Message;
import com.mq.queue.IMessageQueue;
import com.mq.queue.MessageQueue;

import java.util.List;

public class MessageBroker {

    private final IMessageQueue messageQueue;
    private final IDeadLetterQueue dlq;

    public MessageBroker(int maxRetries) {
        this(maxRetries, 1000);
    }

    public MessageBroker(int maxRetries, long backoffBaseMs) {
        this.dlq = new DeadLetterQueue();
        this.messageQueue = new MessageQueue(maxRetries, dlq, backoffBaseMs);
    }

    public String publish(Object payload) {
        return messageQueue.publish(payload);
    }

    public Message consume(long visibilityTimeoutMs) {
        return messageQueue.consume(visibilityTimeoutMs);
    }

    public void ack(String messageId) {
        messageQueue.ack(messageId);
    }

    public void nack(String messageId) {
        messageQueue.nack(messageId);
    }

    public List<Message> getDlqMessages() {
        return dlq.getMessages();
    }

    public void replayOne() {
        Message message = dlq.poll();
        if (message == null) return;
        messageQueue.requeue(message);
    }

    public void shutdown() {
        ((MessageQueue) messageQueue).shutdown();
    }

    public int replayAll() {
        int count = 0;
        Message message;
        while ((message = dlq.poll()) != null) {
            messageQueue.requeue(message);
            count++;
        }
        return count;
    }
}
