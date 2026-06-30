package com.mq.dlq;

import com.mq.model.Message;

import java.util.List;

public interface IDeadLetterQueue {
    void add(Message message);
    Message poll();
    List<Message> getMessages();
}
