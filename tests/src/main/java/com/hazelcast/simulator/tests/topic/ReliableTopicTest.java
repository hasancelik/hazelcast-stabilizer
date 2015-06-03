package com.hazelcast.simulator.tests.topic;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class ReliableTopicTest {

    private static final ILogger LOGGER = Logger.getLogger(ReliableTopicTest.class);

    // properties
    public int topicCount = 10;
    public int threadCount = 3;
    public int listenersPerTopic = 2;
    public String basename = "reliableTopic";

    private IAtomicLong totalExpectedCounter;
    private ITopic[] topics;
    private TestContext testContext;
    private HazelcastInstance hz;
    private List<StressMessageListener> listeners;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        final Random random = new Random();

        this.testContext = testContext;
        hz = testContext.getTargetInstance();
        totalExpectedCounter = hz.getAtomicLong(testContext.getTestId() + ":TotalExpectedCounter");
        topics = new ITopic[topicCount];
        listeners = new LinkedList<StressMessageListener>();

        int listenerIdCounter = 0;
        for (int k = 0; k < topics.length; k++) {
            ITopic<MessageEntity> topic = hz.getReliableTopic(basename + "-" + k);
            topics[k] = topic;
            for (int l = 0; l < listenersPerTopic; l++) {
                StressMessageListener topicListener = new StressMessageListener(listenerIdCounter);
                listenerIdCounter++;
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    private class Worker extends AbstractMonotonicWorker {
        final Random random = new Random();
        long count = 0;
        long totalCounter = 0;

        @Override
        protected void timeStep() {
            ITopic topic = getRandomTopic();
            MessageEntity msg = new MessageEntity(Thread.currentThread().toString(), count);
            totalCounter += msg.counter;
            count++;
            topic.publish(msg);
        }

        @Override
        protected void afterRun() {
            totalExpectedCounter.addAndGet(totalCounter);
        }

        private ITopic getRandomTopic() {
            int index = random.nextInt(topics.length);
            return topics[index];
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    @Verify(global = true)
    public void verify() {

        final long expectedCount = totalExpectedCounter.get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long actualCount = 0;
                for (StressMessageListener topicListener : listeners) {
                    actualCount += topicListener.received;
                }
                assertEquals("published messages don't match received messages", expectedCount, actualCount);
            }
        });
    }

    public static class MessageDataSerializableFactory implements DataSerializableFactory {

        public static final int FACTORY_ID = 18;

        @Override
        public IdentifiedDataSerializable create(int i) {
            return new MessageEntity();
        }
    }

    private static class MessageEntity implements IdentifiedDataSerializable {
        private String thread;
        private long counter;

        public MessageEntity() {
        }

        public MessageEntity(String thread, long counter) {
            this.thread = thread;
            this.counter = counter;
        }

        @Override
        public String toString() {
            return "MessageEntity{" +
                    "thread=" + thread +
                    ", counter=" + counter +
                    '}';
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
            objectDataOutput.writeUTF(thread);
            objectDataOutput.writeLong(counter);

        }

        @Override
        public void readData(ObjectDataInput objectDataInput) throws IOException {
            thread = objectDataInput.readUTF();
            counter = objectDataInput.readLong();
        }

        @Override
        public int getFactoryId() {
            return MessageDataSerializableFactory.FACTORY_ID;
        }

        @Override
        public int getId() {
            return 0;
        }
    }

    private static class StressMessageListener implements MessageListener<MessageEntity> {
        private final int id;
        private long received = 0;

        public StressMessageListener(int id) {
            this.id = id;
        }

        @Override
        public void onMessage(Message<MessageEntity> message) {

            if (received % 100000 == 0) {
                LOGGER.info(toString() + " is at: " + message.toString());
            }

            received += message.getMessageObject().counter;
        }

        @Override
        public String toString() {
            return "StressMessageListener{" +
                    "id=" + id +
                    '}';
        }
    }

}
