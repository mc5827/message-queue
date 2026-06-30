package com.mq.dlq;

import com.mq.dlq.DeadLetterQueue;
import com.mq.model.Message;
import com.mq.model.MessageState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterQueueTest {

    private DeadLetterQueue dlq;

    @BeforeEach
    void setUp() {
        dlq = new DeadLetterQueue();
    }

    // --- add / getMessages ---

    @Test
    void add_messageAppearsInGetMessages() {
        Message message = new Message("payload");
        dlq.add(message);
        assertEquals(1, dlq.getMessages().size());
        assertEquals(message, dlq.getMessages().get(0));
    }

    @Test
    void add_multipleMessages_maintainsFIFOOrder() {
        Message m1 = new Message("first");
        Message m2 = new Message("second");
        Message m3 = new Message("third");
        dlq.add(m1);
        dlq.add(m2);
        dlq.add(m3);

        List<Message> messages = dlq.getMessages();
        assertEquals(m1, messages.get(0));
        assertEquals(m2, messages.get(1));
        assertEquals(m3, messages.get(2));
    }

    @Test
    void getMessages_returnsSnapshot_doesNotMutateDLQ() {
        dlq.add(new Message("payload"));
        List<Message> snapshot = dlq.getMessages();
        assertThrows(UnsupportedOperationException.class, () -> snapshot.remove(0));
        assertEquals(1, dlq.getMessages().size());
    }

    @Test
    void getMessages_emptyDLQ_returnsEmptyList() {
        assertTrue(dlq.getMessages().isEmpty());
    }

    // --- poll ---

    @Test
    void poll_emptyDLQ_returnsNull() {
        assertNull(dlq.poll());
    }

    @Test
    void poll_returnsHeadAndRemovesIt() {
        Message m1 = new Message("first");
        Message m2 = new Message("second");
        dlq.add(m1);
        dlq.add(m2);

        assertEquals(m1, dlq.poll());
        assertEquals(1, dlq.getMessages().size());
        assertEquals(m2, dlq.getMessages().get(0));
    }

    @Test
    void poll_untilEmpty_returnsNull() {
        dlq.add(new Message("payload"));
        dlq.poll();
        assertNull(dlq.poll());
    }

    @Test
    void add_deadMessage_stateIsPreserved() {
        Message message = new Message("payload");
        message.setState(MessageState.DEAD);
        dlq.add(message);
        assertEquals(MessageState.DEAD, dlq.getMessages().get(0).getState());
    }
}
