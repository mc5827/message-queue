package com.mq.queue;

import com.mq.broker.MessageBroker;
import com.mq.model.Message;
import com.mq.model.MessageState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VisibilityTimeoutTest {

    private MessageBroker broker;
    private static final int MAX_RETRIES = 3;
    private static final long SHORT_TIMEOUT_MS = 100;

    @BeforeEach
    void setUp() {
        broker = new MessageBroker(MAX_RETRIES, 0);
    }

    @AfterEach
    void tearDown() {
        broker.shutdown();
    }

    @Test
    void timeout_messageRequeued_availableForNextConsumer() throws InterruptedException {
        broker.publish("hello");
        broker.consume(SHORT_TIMEOUT_MS);

        Thread.sleep(SHORT_TIMEOUT_MS * 2);

        Message requeued = broker.consume(SHORT_TIMEOUT_MS);
        assertNotNull(requeued);
        assertEquals("hello", requeued.getPayload());
    }

    @Test
    void timeout_incrementsRetryCount() throws InterruptedException {
        broker.publish("hello");
        broker.consume(SHORT_TIMEOUT_MS);

        Thread.sleep(SHORT_TIMEOUT_MS * 2);

        Message requeued = broker.consume(SHORT_TIMEOUT_MS);
        assertEquals(1, requeued.getRetryCount());
    }

    @Test
    void timeout_afterAck_doesNotRequeue() throws InterruptedException {
        String id = broker.publish("hello");
        broker.consume(SHORT_TIMEOUT_MS);
        broker.ack(id);

        Thread.sleep(SHORT_TIMEOUT_MS * 2);

        assertNull(broker.consume(SHORT_TIMEOUT_MS));
    }

    @Test
    void timeout_afterNack_doesNotDoubleRequeue() throws InterruptedException {
        String id = broker.publish("hello");
        broker.consume(SHORT_TIMEOUT_MS);
        broker.nack(id);
        Thread.sleep(20);

        // consume the nack-requeued message and ack it
        Message requeued = broker.consume(SHORT_TIMEOUT_MS * 10);
        broker.ack(requeued.getId());

        Thread.sleep(SHORT_TIMEOUT_MS * 2);

        // timeout fired but message was already QUEUED from nack — no double requeue
        assertNull(broker.consume(SHORT_TIMEOUT_MS));
    }

    @Test
    void timeout_exhaustRetries_movesToDLQ() throws InterruptedException {
        broker.publish("hello");

        for (int i = 0; i < MAX_RETRIES; i++) {
            broker.consume(SHORT_TIMEOUT_MS);
            Thread.sleep(SHORT_TIMEOUT_MS * 2);
        }

        assertNull(broker.consume(SHORT_TIMEOUT_MS));
        assertEquals(1, broker.getDlqMessages().size());
        assertEquals(MessageState.DEAD, broker.getDlqMessages().get(0).getState());
    }
}
