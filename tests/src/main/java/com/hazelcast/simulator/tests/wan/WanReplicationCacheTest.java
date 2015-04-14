package com.hazelcast.simulator.tests.wan;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.util.CacheConcurrentHashMap;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static junit.framework.TestCase.assertEquals;

public class WanReplicationCacheTest {

    private enum Operation {
        PUT,
        GET
    }

    private static final ILogger log = Logger.getLogger(WanReplicationCacheTest.class);

    public int threadCount = 3;
    public int cacheCount = 5;
    public int keyCount = 100;
    public boolean active = true;
    public double putProb = 0.80;
    public String wanRepCacheName = "wanRepCache";
    public String activeValuePostFix = "activeX";

    private CacheManager cacheManager;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder();


    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);

        if (isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }
    }

    @Warmup(global = true)
    public void warmup() {
        for(int i = 0; i < cacheCount; i++){
            Cache cache = cacheManager.getCache(wanRepCacheName + "" + i);
            for(int key = 0; key < keyCount; key++){
                cache.put(key, key + activeValuePostFix);
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
            int cacheIdx = randomInt(cacheCount);
            int key = randomInt(keyCount);

            if (active) {
                Cache<Integer, String> cache = cacheManager.getCache(wanRepCacheName + "" + cacheIdx);
                switch (operation) {
                    case PUT:
                        cache.put(key, key + activeValuePostFix);
                        break;
                    case GET:
                        cache.get(key);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        log.info("cluster size = " + targetInstance.getCluster().getMembers().size());
        log.info("replication active = " + active);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                for (int i = 0; i < cacheCount; i++) {
                    ICache cache = (ICache) cacheManager.getCache(wanRepCacheName + "" + i);
                    assertEquals("cacheName=" + cache.getName() + " size ", keyCount, cache.size());
                    for (int key = 0; key < cacheCount; key++) {
                        assertEquals("cacheName=" + cache.getName() + " key=" + key, key + activeValuePostFix, cache.get(key));
                    }
                }
            }
        });
    }

}
