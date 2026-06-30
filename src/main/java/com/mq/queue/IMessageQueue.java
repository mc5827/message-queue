package com.mq.queue;

import com.mq.model.Message;

public interface IMessageQueue {
    String publish(Object payload);
    Message consume(long visibilityTimeoutMs);
    void ack(String messageId);
    void nack(String messageId);
    void requeue(Message message);
}
