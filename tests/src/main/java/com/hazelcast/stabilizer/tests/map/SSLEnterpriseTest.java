package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.stabilizer.test.utils.TestUtils.assertEqualsByteArray;

public class SSLEnterpriseTest {

    private final static ILogger log = Logger.getLogger(SSLEnterpriseTest.class);
    private static Random random = new Random();

    //properties
    public String basename = "sslEnterprise";
    public int threadCount = 3;
    public int keyCount = 1000;
    public int keyLength = 5;
    public KeyLocality keyLocality = KeyLocality.Local;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<String , byte[]> map;

    private static final int[] byteSize = {16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024};
    private final String[] keys = KeyUtils.generateStringKeys(keyCount, keyLength, keyLocality, targetInstance);

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
        log.info("Map size is:" + map.size());
        log.info("Total Value MB is:" + totalByte / (1024 * 1024));
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i < keyCount; i++) {
                String key = keys[random.nextInt(keys.length)];
                try {
                    map.put(key, createValue(key));
                } catch (IOException e) {
                }
            }
        }
    }

    private byte[] createValue(String key) throws IOException {
        int valueLength = byteSize[random.nextInt(byteSize.length)];
        byte[] valuePart = new byte[valueLength];
        byte[] keyPart = key.getBytes();

        return ArrayUtils.addAll(keyPart, valuePart);
    }

}
