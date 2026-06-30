package com.mq.broker;

import com.mq.broker.MessageBroker;
import com.mq.model.Message;
import com.mq.model.MessageState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MessageBrokerTest {

    private MessageBroker broker;
    private static final int MAX_RETRIES = 3;
    private static final long VISIBILITY_TIMEOUT_MS = 5000;
    private static final long SCHEDULER_SETTLE_MS = 20;

    @BeforeEach
    void setUp() {
        broker = new MessageBroker(MAX_RETRIES, 0);
    }

    private String exhaustRetries(String messageId) throws InterruptedException {
        for (int i = 0; i < MAX_RETRIES; i++) {
            broker.consume(VISIBILITY_TIMEOUT_MS);
            broker.nack(messageId);
            Thread.sleep(SCHEDULER_SETTLE_MS);
        }
        return messageId;
    }

    // --- replayOne ---

    @Test
    void replayOne_emptyDLQ_doesNothing() {
        assertDoesNotThrow(() -> broker.replayOne());
        assertNull(broker.consume(VISIBILITY_TIMEOUT_MS));
    }

    @Test
    void replayOne_movesMessageBackToMainQueue() throws InterruptedException {
        String id = broker.publish("hello");
        exhaustRetries(id);

        broker.replayOne();

        Message replayed = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertNotNull(replayed);
        assertEquals(id, replayed.getId());
    }

    @Test
    void replayOne_resetsRetryCount() throws InterruptedException {
        String id = broker.publish("hello");
        exhaustRetries(id);

        broker.replayOne();

        Message replayed = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertEquals(0, replayed.getRetryCount());
    }

    @Test
    void replayOne_removesMesageFromDLQ() throws InterruptedException {
        String id = broker.publish("hello");
        exhaustRetries(id);

        broker.replayOne();

        assertTrue(broker.getDlqMessages().isEmpty());
    }

    @Test
    void replayOne_onlyReplayHead_leaveRestInDLQ() throws InterruptedException {
        String id1 = broker.publish("first");
        exhaustRetries(id1);
        String id2 = broker.publish("second");
        exhaustRetries(id2);

        broker.replayOne();

        assertEquals(1, broker.getDlqMessages().size());
        Message replayed = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertEquals(id1, replayed.getId());
    }

    @Test
    void replayOne_replayedMessage_canBeNackedAndSentToDLQAgain() throws InterruptedException {
        String id = broker.publish("hello");
        exhaustRetries(id);
        broker.replayOne();

        exhaustRetries(id);

        assertEquals(1, broker.getDlqMessages().size());
        assertEquals(MessageState.DEAD, broker.getDlqMessages().get(0).getState());
    }

    // --- replayAll ---

    @Test
    void replayAll_emptyDLQ_returnsZero() {
        assertEquals(0, broker.replayAll());
    }

    @Test
    void replayAll_movesAllMessagesBackToMainQueue() throws InterruptedException {
        String id1 = broker.publish("first");
        exhaustRetries(id1);
        String id2 = broker.publish("second");
        exhaustRetries(id2);

        int count = broker.replayAll();

        assertEquals(2, count);
        assertTrue(broker.getDlqMessages().isEmpty());
        assertNotNull(broker.consume(VISIBILITY_TIMEOUT_MS));
        assertNotNull(broker.consume(VISIBILITY_TIMEOUT_MS));
    }

    @Test
    void replayAll_resetsRetryCountOnAllMessages() throws InterruptedException {
        String id1 = broker.publish("first");
        exhaustRetries(id1);
        String id2 = broker.publish("second");
        exhaustRetries(id2);

        broker.replayAll();

        Message m1 = broker.consume(VISIBILITY_TIMEOUT_MS);
        Message m2 = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertEquals(0, m1.getRetryCount());
        assertEquals(0, m2.getRetryCount());
    }
}
