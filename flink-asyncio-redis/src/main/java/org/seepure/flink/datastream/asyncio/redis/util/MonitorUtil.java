package org.seepure.flink.datastream.asyncio.redis.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorUtil {

    private static Logger LOG = LoggerFactory.getLogger(MonitorUtil.class);
    private static volatile boolean running = true;

    public static Thread createCacheMonitorThread(Cache<String, Object> cache) {
        AssertUtil.assertTrue(cache != null, "cache is null!");
        return new Thread(() -> {
            while (running) {
                try {
                    CacheStats stats = cache.stats();
                    LOG.info(stats.toString());
                    Thread.sleep(10_000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void stop() {
        running = false;
    }

}
