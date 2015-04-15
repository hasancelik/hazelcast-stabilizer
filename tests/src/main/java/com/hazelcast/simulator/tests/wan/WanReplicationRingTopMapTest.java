package com.hazelcast.simulator.tests.wan;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static junit.framework.TestCase.assertEquals;

public class WanReplicationRingTopMapTest {
    private static final ILogger log = Logger.getLogger(WanReplicationRingTopMapTest.class);

    public int threadCount = 3;
    public int keyCount = 10;
    public int keyRangeStart = 0;
    public int clusterCount = 3;

    public String wanRepMapName = "repMap";

    private TestContext testContext;
    private HazelcastInstance targetInstance;


    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() {
        IMap map = targetInstance.getMap(wanRepMapName);
        for (int key = keyRangeStart; key < keyRangeStart + keyCount; key++) {
            map.put(key, key);
        }
    }

    @RunWithWorker
    public AbstractWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        @Override
        protected void timeStep() {

        }
    }

    @Verify(global = false)
    public void verify() throws Exception {

        IMap map = targetInstance.getMap(wanRepMapName);
        log.info("keys "+map.keySet());

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                IMap map = targetInstance.getMap(wanRepMapName);
                assertEquals("mapName=" + map.getName() + " size ", clusterCount * keyCount, map.size());
                for (int key = 0; key < clusterCount * keyCount; key++) {
                    assertEquals("mapName=" + map.getName() + " key=" + key, key, map.get(key));
                }
            }
        });
    }
}