package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.*;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SSLTest {

    public static final int[] byteSize = {16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024};
    private final static ILogger log = Logger.getLogger(SSLTest.class);
    public static Random random = new Random();
    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 1000;
    public int keyLength = 10;
    public KeyLocality keyLocality = KeyLocality.Local;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<String, byte[]> map;

    static {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "ENPDHM9KIAB45600S318R01S0W5162");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    public static void main(String[] args) throws Throwable {
        SSLTest test = new SSLTest();
        new TestRunner<SSLTest>(test).run();
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

    @Verify
    public void verify() {
        int totalByte = 0;
        for(String k: map.keySet()){
            byte[] key = k.getBytes();
            byte[] value = map.get(k);
            totalByte += value.length;
            assertEquals(key, ArrayUtils.subarray(value, 0, 9));
        }
        log.info("Map size is:" + map.size());
        log.info("Total Value KB is:" + totalByte / 1024);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i < keyCount; i++) {
                String key = KeyUtils.generateStringKey(keyLength, keyLocality, targetInstance);
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
