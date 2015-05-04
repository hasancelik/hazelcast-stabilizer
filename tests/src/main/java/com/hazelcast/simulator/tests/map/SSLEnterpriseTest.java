package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.simulator.utils.TestUtils.assertEqualsByteArray;

/**
 * This test provides functionality of stressing for SSL feature of Hazelcast Enterprise
 */
public class SSLEnterpriseTest {

    private static final ILogger LOGGER = Logger.getLogger(SSLEnterpriseTest.class);
    private static final int[] BYTE_SIZE = {16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024};
    //properties
    public String basename = "sslEnterprise";
    public int threadCount = 3;
    public int keyCount = 1000;
    public int keyLength = 5;
    public KeyLocality keyLocality = KeyLocality.RANDOM;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private final String[] keys = KeyUtils.generateStringKeys(keyCount, keyLength, keyLocality, targetInstance);
    private IMap<String, byte[]> map;

    public static void main(String[] args) throws Throwable {
        SSLEnterpriseTest test = new SSLEnterpriseTest();
        new TestRunner<SSLEnterpriseTest>(test).run();
    }

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify(global = false)
    public void verify() {
        double totalByte = 0;
        for(String k : map.keySet()){
            byte[] key = k.getBytes();
            byte[] value = map.get(k);
            totalByte += value.length;
            assertEqualsByteArray(key, Arrays.copyOfRange(value, 0, keyLength));
        }
        LOGGER.info("Map size is:" + map.size());
        LOGGER.info("Total Value MB is:" + totalByte / (1024 * 1024));
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        @Override
        public void run() {
            while (!testContext.isStopped()) {
                String key = keys[random.nextInt(keys.length)];
                map.put(key, createValue(key,random));
            }
        }
    }

    /**
     * This method produces verifiable value that includes key.
     * This approach help to recognize the key-value pair after SSL operation.
     * */
    private byte[] createValue(String key,Random random){
        int valueLength = BYTE_SIZE[random.nextInt(BYTE_SIZE.length)];
        byte[] valuePart = new byte[valueLength];
        byte[] keyPart = key.getBytes();

        return ArrayUtils.addAll(keyPart, valuePart);
    }

}
