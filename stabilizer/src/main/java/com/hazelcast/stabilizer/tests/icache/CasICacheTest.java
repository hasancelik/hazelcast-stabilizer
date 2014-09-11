package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.cache.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.CacheException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * This tests the cas method: replace. So for optimistic concurrency control.
 * <p/>
 * We have a bunch of predefined keys, and we are going to concurrently increment the value
 * and we protect ourselves against lost updates using cas method replace.
 * <p/>
 * Locally we keep track of all increments, and if the sum of these local increments matches the
 * global increment, we are done
 */
public class CasICacheTest {

    private final static ILogger log = Logger.getLogger(CasICacheTest.class);

    //props
    public int threadCount = 10;
    public int keyCount = 1000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = "icachecas";

    private final AtomicLong operations = new AtomicLong();
    private IMap<String, Map<Integer, Long>> resultsPerWorker;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private ICache<Integer, Long> cache;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;

        targetInstance = testContext.getTargetInstance();

        HazelcastCacheManager cacheManager;
        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
        config.setName(basename);
        config.setTypes(Integer.class, Long.class);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            //temp hack to deal with multiple nodes wanting to make the same cache.
            log.severe(hack);
        }

        cache = cacheManager.getCache(basename, Integer.class, Long.class);
        resultsPerWorker = targetInstance.getMap("ResultMap" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        cache.close();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0l);
        }
    }

    @Run
    public void run() {
        if (cache.size() != keyCount) {
            throw new RuntimeException("warmup has not run since the map is not filled correctly, found size:" + cache.size());
        }

        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() throws Exception {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> map : resultsPerWorker.values()) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            long expected = amount[k];
            long found = cache.get(k);
            if (expected != found) {
                failures++;
            }
        }

        assertEquals("There should not be any data races", 0, failures);
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        @Override
        public void run() {
            for (int k = 0; k < keyCount; k++) {
                result.put(k, 0L);
            }

            long iteration = 0;
            while (!testContext.isStopped()) {
                Integer key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                for (; ; ) {
                    Long current = cache.get(key);
                    Long update = current + increment;
                    if (cache.replace(key, current, update)) {
                        increment(key, increment);
                        break;
                    }
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }

            resultsPerWorker.put(UUID.randomUUID().toString(), result);
        }

        private void increment(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    public static void main(String[] args) throws Throwable {
        CasICacheTest test = new CasICacheTest();
        new TestRunner(test).run();
    }
}