package com.hazelcast.simulator.tests.topic;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.topic.ReliableMessageListener;

import static com.hazelcast.simulator.utils.CommonUtils.sleepRandomNanos;
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
        this.testContext = testContext;
        hz = testContext.getTargetInstance();

        totalExpectedCounter = hz.getAtomicLong(testContext.getTestId() + ":TotalExpectedCounter");
        topics = new ITopic[topicCount];
        listeners = new LinkedList<StressMessageListener>();
        int listenerIdCounter = 0;
        for (int k = 0; k < topics.length; k++) {
            ITopic<Long> topic = hz.getReliableTopic(basename + "-" + k);
            topics[k] = topic;
            for (int l = 0; l < listenersPerTopic; l++) {
                StressMessageListener topicListener = new StressMessageListener(listenerIdCounter);
                listenerIdCounter++;
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long count = 0;
            while (!testContext.isStopped()) {

                ITopic topic = getRandomTopic();
                long msg = nextMessage();
                count += msg;
                topic.publish(msg);
            }
            totalExpectedCounter.addAndGet(count);
        }

        private ITopic getRandomTopic() {
            int index = random.nextInt(topics.length);
            return topics[index];
        }

        private long nextMessage() {
            long msg = random.nextLong() % 1000;
            if (msg < 0) {
                msg = -msg;
            }
            return msg;
        }
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

    public class StressMessageListener implements ReliableMessageListener<Long> {
        private final int id;
        private long received = 0;

        public StressMessageListener(int id) {
            this.id = id;
        }

        @Override
        public void onMessage(Message<Long> message) {
            final Random random = new Random();

            if (received % 100000 == 0) {
                LOGGER.info(toString() + " is at: " + received);
            }

            received += message.getMessageObject();
        }

        @Override
        public String toString() {
            return "StressMessageListener{" +
                    "id=" + id +
                    '}';
        }

        @Override
        public long retrieveInitialSequence() {
            return 0;
        }

        @Override
        public void storeSequence(long l) {

        }

        @Override
        public boolean isLossTolerant() {
            return false;
        }

        @Override
        public boolean isTerminal(Throwable throwable) {
            return false;
        }
    }

}
