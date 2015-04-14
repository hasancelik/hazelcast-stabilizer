package com.hazelcast.simulator.tests.wan;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static junit.framework.TestCase.assertEquals;

public class WanReplicationMapTest {

    private enum Operation {
        PUT,
        GET
    }

    private static final ILogger log = Logger.getLogger(WanReplicationMapTest.class);

    public int threadCount = 3;
    public int mapCount = 10;
    public int keyCount = 100;
    public boolean active = true;
    public boolean passiveMapLoad = true;

    public double putProb = 0.75;

    public String wanRepMapName = "repMap";
    public String passiveMap = "passiveMap";

    public String activeValuePostFix = "activeX";
    public String passiveValuePostFix = "passiveX";


    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }

    @Warmup(global = true)
    public void warmup() {
        if (active) {
            for (int i = 0; i < mapCount; i++) {
                IMap map = targetInstance.getMap(wanRepMapName + "" + i);
                for (int key = 0; key < keyCount; key++) {
                    map.put(key, key + activeValuePostFix);
                }
            }
        }
    }

    @RunWithWorker
    public AbstractWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) {
            int mapIdx = randomInt(mapCount);
            int key = randomInt(keyCount);

            if (active) {
                IMap map = targetInstance.getMap(wanRepMapName + "" + mapIdx);
                switch (operation) {
                    case PUT:
                        map.put(key, key + activeValuePostFix);
                        break;
                    case GET:
                        map.get(key);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } else if (passiveMapLoad) {
                IMap map = targetInstance.getMap(passiveMap + "" + mapIdx);
                map.put(key, key + passiveValuePostFix);
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        log.info("cluster size = " + targetInstance.getCluster().getMembers().size());
        log.info("replication active = " + active);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                for (int i = 0; i < mapCount; i++) {
                    IMap map = targetInstance.getMap(wanRepMapName + "" + i);
                    assertEquals("mapName=" + map.getName() + " size ", keyCount, map.size());
                    for (int key = 0; key < mapCount; key++) {
                        assertEquals("mapName=" + map.getName() + " key=" + key, key + activeValuePostFix, map.get(key));
                    }
                }
            }
        });
    }

    public static void main(String[] args) throws Throwable {
        WanReplicationMapTest test = new WanReplicationMapTest();
        new TestRunner<WanReplicationMapTest>(test).run();
    }
}