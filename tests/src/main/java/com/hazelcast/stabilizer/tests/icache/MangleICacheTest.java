package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.OperationSelector;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.isMemberNode;

/**
 * In This tests we are intentionally creating destroying closing and using, cache managers and there caches
 * this type of cache usage is well out side normal usage however we did find 2 bugs with this test.  This
 * test could high light memory leeks when repeatedly creating an destroying cache's / managers, some thing
 * that regular test would not normalcy find.
 */
public class MangleICacheTest {

    public static enum Operation {
        CLOSE_CACHING_PROVIDER,

        CREATE_CACHE_MANAGER,
        CLOSE_CACHE_MANAGER,

        CREATE_CACHE,
        CLOSE_CACHE,
        DESTROY_CACHE,

        PUT
    }

    private final static ILogger log = Logger.getLogger(MangleICacheTest.class);

    public int threadCount = 3;
    public int maxCaches = 100;

    public double createCacheManager=0.1;
    public double cacheManagerClose=0.1;
    public double cachingProviderClose=0.1;

    public double createCacheProb=0.1;
    public double destroyCacheProb=0.2;
    public double putCacheProb=0.3;
    public double closeCacheProb=0.1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String basename;

    private OperationSelector<Operation> selector;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        selector = new OperationSelector<Operation>();
        selector.addOperation(Operation.CLOSE_CACHING_PROVIDER, cachingProviderClose)
                .addOperation(Operation.CREATE_CACHE_MANAGER, createCacheManager)
                .addOperation(Operation.CLOSE_CACHE_MANAGER, cacheManagerClose)
                .addOperation(Operation.CREATE_CACHE, createCacheProb)
                .addOperation(Operation.CLOSE_CACHE, closeCacheProb)
                .addOperation(Operation.DESTROY_CACHE, destroyCacheProb)
                .addOperation(Operation.PUT, putCacheProb);
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
        private final CacheConfig config = new CacheConfig();
        private final Counter counter = new Counter();
        private CacheManager cacheManager=null;

        public void run() {
            config.setName(basename);
            createCacheManager();

            while (!testContext.isStopped()) {
                int cacheNumber = random.nextInt(maxCaches);

                Operation operation = selector.select();
                switch (operation) {
                    case CLOSE_CACHING_PROVIDER: {
                        try {
                            CachingProvider provider = cacheManager.getCachingProvider();
                            if (provider != null) {
                                provider.close();
                                counter.cachingProviderClose++;
                            }
                        } catch (CacheException e) {
                            counter.cachingProviderCloseException++;
                        }
                        break;
                    }
                    case CREATE_CACHE_MANAGER: {
                        try {
                            createCacheManager();
                            counter.createCacheManager++;

                        } catch (CacheException e) {
                            counter.createCacheManagerException++;

                        }
                        break;
                    }
                    case CLOSE_CACHE_MANAGER: {
                        try {
                            cacheManager.close();
                            counter.cacheManagerClose++;

                        } catch (CacheException e) {
                            counter.cacheManagerCloseException++;

                        }
                        break;
                    }
                    case CREATE_CACHE: {
                        try {
                            cacheManager.createCache(basename + cacheNumber, config);
                            counter.create++;

                        } catch (CacheException e) {
                            counter.createException++;

                        } catch (IllegalStateException e) {
                            counter.createException++;

                        }
                        break;
                    }
                    case CLOSE_CACHE: {
                        Cache cache = getCacheIfExists(cacheNumber);
                        try {
                            if (cache != null) {
                                cache.close();
                                counter.cacheClose++;
                            }
                        } catch (CacheException e) {
                            counter.cacheCloseException++;

                        } catch (IllegalStateException e) {
                            counter.cacheCloseException++;

                        }
                        break;
                    }
                    case DESTROY_CACHE: {
                        try {
                            cacheManager.destroyCache(basename + cacheNumber);
                            counter.destroy++;

                        } catch (CacheException e) {
                            counter.destroyException++;

                        } catch (IllegalStateException e) {
                            counter.destroyException++;

                        }
                        break;
                    }
                    case PUT: {
                        Cache cache = getCacheIfExists(cacheNumber);
                        try {
                            if (cache != null) {
                                cache.put(random.nextInt(), random.nextInt());
                                counter.put++;
                            }
                        } catch (CacheException e) {
                            counter.getPutException++;

                        } catch (IllegalStateException e) {
                            counter.getPutException++;

                        }
                        break;
                    }
                }
            }
            targetInstance.getList(basename).add(counter);
        }

        public void createCacheManager(){
            CachingProvider currentCachingProvider = null;
            if (cacheManager != null) {
                currentCachingProvider = cacheManager.getCachingProvider();
                cacheManager.close();
            }
            if (isMemberNode(targetInstance)) {
                HazelcastServerCachingProvider hcp = (HazelcastServerCachingProvider) currentCachingProvider;
                if (hcp == null) {
                    hcp = new HazelcastServerCachingProvider();
                }
                cacheManager = new HazelcastServerCacheManager(
                        hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
            } else {
                HazelcastClientCachingProvider hcp = (HazelcastClientCachingProvider) currentCachingProvider;
                if (hcp == null) {
                    hcp = new HazelcastClientCachingProvider();
                }
                cacheManager = new HazelcastClientCacheManager(
                        hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
            }
        }

        public Cache getCacheIfExists(int cacheNumber){
            try{
                Cache cache = cacheManager.getCache(basename + cacheNumber);
                counter.getCache++;
                return cache;

            } catch (CacheException e){
                counter.getCacheException++;

            } catch (IllegalStateException e){
                counter.getCacheException++;

            }
            return null;
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {
        IList<Counter> counters = targetInstance.getList(basename);
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(basename + ": " + total + " from " + counters.size() + " worker threads");
    }

    static class Counter implements Serializable {

        public long createCacheManager = 0;
        public long createCacheManagerException = 0;

        public long getCache = 0;
        public long put = 0;
        public long create = 0;
        public long destroy = 0;
        public long cacheClose = 0;

        public long getCacheException = 0;
        public long getPutException = 0;
        public long createException = 0;
        public long destroyException = 0;
        public long cacheCloseException = 0;

        public long cacheManagerClose=0;
        public long cachingProviderClose=0;

        public long cacheManagerCloseException=0;
        public long cachingProviderCloseException=0;

        public void add(Counter c) {

            createCacheManager += c.createCacheManager;
            createCacheManagerException += c.createCacheManagerException;

            getCache += c.getCache;
            put += c.put;
            create += c.create;
            destroy += c.destroy;
            cacheClose += c.cacheClose;

            getCacheException += c.getCacheException;
            getPutException += c.getPutException;
            createException += c.createException;
            destroyException += c.destroyException;
            cacheCloseException += c.cacheCloseException;

            cacheManagerClose += c.cacheManagerClose;
            cachingProviderClose += c.cachingProviderClose;
            cacheManagerCloseException += c.cacheManagerCloseException;
            cachingProviderCloseException += c.cachingProviderCloseException;
        }

        public String toString() {
            return "Counter{" +
                    "createCacheManager=" + createCacheManager +
                    ", createCacheManagerException=" + createCacheManagerException +
                    ", getCache=" + getCache +
                    ", put=" + put +
                    ", create=" + create +
                    ", destroy=" + destroy +
                    ", cacheClose=" + cacheClose +
                    ", getCacheException=" + getCacheException +
                    ", getPutException=" + getPutException +
                    ", createException=" + createException +
                    ", destroyException=" + destroyException +
                    ", cacheCloseException=" + cacheCloseException +
                    ", cacheManagerClose=" + cacheManagerClose +
                    ", cachingProviderClose=" + cachingProviderClose +
                    ", cacheManagerCloseException=" + cacheManagerCloseException +
                    ", cachingProviderCloseException=" + cachingProviderCloseException +
                    '}';
        }
    }
}