# Async Message Queue with Dead Letter Queue (DLQ)

## Problem Statement

Design and implement a production-ready **asynchronous message queue system** from scratch in **Java**.

The system must support:

- **Message publishing** — producers enqueue messages into the queue
- **Acknowledgment-based processing** — consumers explicitly ACK or NACK each message
- **Retry mechanism** — failed messages (NACK or timeout) are automatically requeued up to a configurable max retry count
- **Dead Letter Queue (DLQ)** — messages that exceed max retries are moved to a separate DLQ instead of being dropped
- **DLQ replay** — messages in the DLQ can be replayed back into the main queue for reprocessing

---

## Core Concepts

### Acknowledgment-Based Processing

When a consumer picks up a message, the queue does **not** delete it immediately. Instead:

- The message is marked **IN_FLIGHT** (invisible to other consumers) for a configurable **visibility timeout**
- If the consumer calls `ack(messageId)` → message is permanently deleted ✅
- If the consumer calls `nack(messageId)` OR the visibility timeout expires → message is returned to the queue for retry ❌

### Retry Flow

```
Consumer pulls message
    └── Processes it
           ├── ACK  → message deleted permanently ✓
           └── NACK → retryCount++
                    ├── retryCount < maxRetries  → requeue (back to main queue)
                    └── retryCount >= maxRetries → move to DLQ
```

### Dead Letter Queue (DLQ)

The DLQ is a separate queue that holds messages that have failed processing too many times. It allows:

- **Inspection** — understand why messages are failing
- **Replay** — reprocess DLQ messages after fixing the underlying bug

---

## Functional Requirements

### Phase 1 — Core Queue

- `publish(payload)` → enqueue a new message with `retryCount = 0`
- `consume()` → dequeue a message, mark it IN_FLIGHT with a visibility timeout, return it to the caller
- `ack(messageId)` → permanently remove the message
- `nack(messageId)` → requeue or move to DLQ based on retry count
- Messages not ACK-ed within the visibility timeout are automatically re-enqueued

### Phase 2 — Dead Letter Queue

- `getDlqMessages()` → return all messages currently in the DLQ
- `replayAll()` → move all DLQ messages back to the main queue (reset retryCount to 0)
- `replayOne(messageId)` → replay a single DLQ message back to the main queue

### Phase 3 — Thread Safety

- Multiple producers and consumers operating concurrently without data corruption
- No message delivered to two consumers simultaneously
- No race conditions on retryCount, visibility timeout expiry, or DLQ transitions
- Use Java concurrency primitives: `ReentrantLock`, `synchronized`, `BlockingQueue`, or similar

### Phase 4 — Follow-up Discussion

Be ready to discuss:

- How would you handle **idempotency** (consumer processes same message twice)?
- What **backoff strategy** would you use for retries (immediate, fixed delay, exponential)?
- How would you **monitor** queue health (depth, DLQ size, age of oldest message)?
- What happens if a consumer crashes mid-processing?
- How would you scale to **multiple consumers**?

---

## Data Model

```java
enum MessageState {
    QUEUED,      // waiting in the main queue
    IN_FLIGHT,   // consumed but not yet ACKed/NACKed
    DEAD         // moved to DLQ
}

class Message {
    String id;               // UUID
    Object payload;          // the actual content
    int retryCount;          // failed attempts so far, starts at 0
    int maxRetries;          // configurable, e.g. 3
    long enqueuedAt;         // System.currentTimeMillis()
    long visibilityDeadline; // set when consumed, 0 when QUEUED
    MessageState state;
}
```

---

## Expected Interface

```java
interface IMessageQueue {
    String publish(Object payload);
    String publish(Object payload, int maxRetries);
    Message consume(long visibilityTimeoutMs);
    void ack(String messageId);
    void nack(String messageId);
}

interface IDeadLetterQueue {
    List<Message> getMessages();
    int replayAll();                    // returns count of replayed messages
    void replayOne(String messageId);
}
```

---

## Project Structure

```
message_queue/
├── README.md
├── pom.xml
└── src/
    ├── main/
    │   └── java/
    │       └── com/
    │           └── mq/
    │               ├── model/
    │               │   ├── Message.java
    │               │   └── MessageState.java
    │               ├── queue/
    │               │   ├── IMessageQueue.java
    │               │   └── MessageQueue.java
    │               ├── dlq/
    │               │   ├── IDeadLetterQueue.java
    │               │   └── DeadLetterQueue.java
    │               └── broker/
    │                   └── MessageBroker.java
    └── test/
        └── java/
            └── com/
                └── mq/
                    └── MessageQueueTest.java
```

---

## Build & Run

```bash
# Run all tests
mvn clean test

# Compile only
mvn clean compile
```

---

## Constraints & Assumptions

- In-memory implementation only (no Kafka, RabbitMQ, or external broker)
- Single-node (no distributed concerns unless raised in Phase 4 discussion)
- Java 11+, use `java.util.concurrent` freely
- Unit tests required for each phase — JUnit 5
- Use the AI assistant for boilerplate, but own the design decisions and verify all output

---

## Evaluation Criteria

| Area              | What's Tested |
|-------------------|---------------|
| **Problem Solving** | Deep understanding of ack/nack flow, edge cases (double ACK, ACK after timeout, replay of already-replayed message) |
| **Code Quality**    | Clean class structure, interface-based design, SOLID principles |
| **Verification**    | Unit tests covering happy path, retry exhaustion, DLQ replay, concurrent access |
| **Concurrency**     | Thread-safe queue using Java concurrency primitives (Phase 3) |
| **Communication**   | Explain design decisions and trade-offs clearly |

---

## Test Cases to Cover

```
Phase 1:
  - publish → consume → ack → message gone
  - publish → consume → nack → message requeued (retryCount incremented)
  - publish → consume → nack × maxRetries → message in DLQ

Phase 2:
  - getDlqMessages() returns all dead messages
  - replayOne() moves a single message back with retryCount = 0
  - replayAll() moves all messages back

Phase 3:
  - 10 concurrent producers publishing 100 messages each → no lost messages
  - 5 concurrent consumers → no message delivered twice
  - Visibility timeout expiry → message re-enqueued automatically

Edge Cases:
  - ack on unknown messageId → throws IllegalArgumentException
  - ack on already-ACKed message → throws IllegalStateException
  - consume on empty queue → returns null (or Optional.empty())
  - nack after visibility timeout already expired → handle gracefully
```

---

## How to Approach This (Interview Strategy)

1. **Clarify requirements** — confirm visibility timeout behaviour, whether `maxRetries` is per-message or queue-wide, whether DLQ replay resets retry count
2. **Design the data model first** — sketch `Message` and `MessageState` before writing any queue logic
3. **Implement Phase 1** — single-threaded queue with ack/nack
4. **Write tests** — at least the Phase 1 cases above before moving on
5. **Add Phase 2** — DLQ methods
6. **Add Phase 3** — wrap shared state with `ReentrantLock` or `synchronized`
7. **Discuss trade-offs proactively** — mention idempotency, backoff, monitoring without waiting to be asked

---

Good luck!
