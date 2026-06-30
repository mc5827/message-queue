package com.mq.queue;

import com.mq.broker.MessageBroker;
import com.mq.model.Message;
import com.mq.model.MessageState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MessageQueueTest {

    private MessageBroker broker;
    private static final int MAX_RETRIES = 3;
    private static final long VISIBILITY_TIMEOUT_MS = 5000;
    private static final long SCHEDULER_SETTLE_MS = 20;

    @BeforeEach
    void setUp() {
        broker = new MessageBroker(MAX_RETRIES, 0);
    }

    // --- publish ---

    @Test
    void publish_returnsMessageId() {
        String id = broker.publish("hello");
        assertNotNull(id);
    }

    @Test
    void publish_multipleMessages_returnDistinctIds() {
        String id1 = broker.publish("msg1");
        String id2 = broker.publish("msg2");
        assertNotEquals(id1, id2);
    }

    // --- consume ---

    @Test
    void consume_emptyQueue_returnsNull() {
        assertNull(broker.consume(VISIBILITY_TIMEOUT_MS));
    }

    @Test
    void consume_returnsMessageInFIFOOrder() {
        broker.publish("first");
        broker.publish("second");

        Message first = broker.consume(VISIBILITY_TIMEOUT_MS);
        Message second = broker.consume(VISIBILITY_TIMEOUT_MS);

        assertEquals("first", first.getPayload());
        assertEquals("second", second.getPayload());
    }

    @Test
    void consume_setsMessageToInFlight() {
        broker.publish("hello");
        Message message = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertEquals(MessageState.IN_FLIGHT, message.getState());
    }

    @Test
    void consume_setsVisibilityDeadline() {
        broker.publish("hello");
        long before = System.currentTimeMillis();
        Message message = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertTrue(message.getVisibilityDeadline() >= before + VISIBILITY_TIMEOUT_MS);
    }

    @Test
    void consume_inFlightMessageNotVisibleToOtherConsumers() {
        broker.publish("only");
        broker.consume(VISIBILITY_TIMEOUT_MS);
        assertNull(broker.consume(VISIBILITY_TIMEOUT_MS));
    }

    // --- ack ---

    @Test
    void ack_removesMessagePermanently() {
        String id = broker.publish("hello");
        broker.consume(VISIBILITY_TIMEOUT_MS);
        broker.ack(id);
        assertNull(broker.consume(VISIBILITY_TIMEOUT_MS));
    }

    @Test
    void ack_unknownMessageId_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> broker.ack("unknown-id"));
    }

    @Test
    void ack_onQueuedMessage_throwsIllegalStateException() {
        String id = broker.publish("hello");
        assertThrows(IllegalStateException.class, () -> broker.ack(id));
    }

    @Test
    void ack_alreadyAcked_throwsIllegalArgumentException() {
        String id = broker.publish("hello");
        broker.consume(VISIBILITY_TIMEOUT_MS);
        broker.ack(id);
        assertThrows(IllegalArgumentException.class, () -> broker.ack(id));
    }

    // --- nack ---

    @Test
    void nack_requeueMessage_incrementsRetryCount() throws InterruptedException {
        String id = broker.publish("hello");
        broker.consume(VISIBILITY_TIMEOUT_MS);
        broker.nack(id);
        Thread.sleep(SCHEDULER_SETTLE_MS);

        Message requeued = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertNotNull(requeued);
        assertEquals(1, requeued.getRetryCount());
    }

    @Test
    void nack_requeuedMessage_stateIsQueued() throws InterruptedException {
        String id = broker.publish("hello");
        broker.consume(VISIBILITY_TIMEOUT_MS);
        broker.nack(id);
        Thread.sleep(SCHEDULER_SETTLE_MS);

        Message requeued = broker.consume(VISIBILITY_TIMEOUT_MS);
        assertEquals(MessageState.IN_FLIGHT, requeued.getState());
    }

    @Test
    void nack_exceedsMaxRetries_movesToDLQ() throws InterruptedException {
        String id = broker.publish("hello");

        for (int i = 0; i < MAX_RETRIES; i++) {
            broker.consume(VISIBILITY_TIMEOUT_MS);
            broker.nack(id);
            Thread.sleep(SCHEDULER_SETTLE_MS);
        }

        assertNull(broker.consume(VISIBILITY_TIMEOUT_MS));
        assertEquals(1, broker.getDlqMessages().size());
        assertEquals(MessageState.DEAD, broker.getDlqMessages().get(0).getState());
    }

    @Test
    void nack_unknownMessageId_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> broker.nack("unknown-id"));
    }

    @Test
    void nack_onQueuedMessage_throwsIllegalStateException() {
        String id = broker.publish("hello");
        assertThrows(IllegalStateException.class, () -> broker.nack(id));
    }
}
