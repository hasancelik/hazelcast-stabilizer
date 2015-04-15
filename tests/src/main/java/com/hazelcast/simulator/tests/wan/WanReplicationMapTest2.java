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

public class WanReplicationMapTest2 {

    private enum Operation {
        PUT,
        GET,
        REMOVE
    }

    private static final ILogger log = Logger.getLogger(WanReplicationMapTest2.class);

    public int threadCount = 3;
    public int mapCount = 1;
    public int keyCount = 10;
    public boolean active = true;

    public double putProb = 0.6;
    public double getProb = 0.2;
    public double removeProb = 0.2;
    public int coolDownSleep = 5000;

    public String repMapName = "repMap";
    public String keyPreFix;


    private String id;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        id = testContext.getTestId();
        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                                .addOperation(Operation.GET, getProb)
                                .addOperation(Operation.REMOVE, removeProb);
    }

    @Warmup(global = true)
    public void warmup() { }

    @RunWithWorker
    public AbstractWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        protected void timeStep(Operation operation) {
            int mapIdx = randomInt(mapCount);
            String key = keyPreFix + randomInt(keyCount);

            if (active) {
                IMap map = targetInstance.getMap(repMapName + "" + mapIdx);
                switch (operation) {
                    case PUT:
                        map.put(key, key);
                        break;
                    case GET:
                        map.get(key);
                        break;
                    case REMOVE:
                        map.remove(key);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        Thread.sleep(coolDownSleep);

        log.info(id+": replication active = " + active);

        for (int i=0; i<mapCount; i++) {
            IMap map = targetInstance.getMap(repMapName + "" + i);
            log.info(id+": mapName=" + map.getName() + " size=" + map.size());
            log.info(id+": keySet=" + map.keySet());
        }
    }
}