package org.seepure.flink.datastream.asyncio.redis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisKvTextSchema;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ErrorCheckerSink extends RichSinkFunction<String> {
    private static Logger LOG = LoggerFactory.getLogger(ErrorCheckerSink.class);
    private AtomicLong throughputCounter;
    private AtomicInteger errorCounter;
    private DimRedisKvTextSchema dimRedisKvTextSchema;
    private String keyColumn;
    private long lastTime;
    private Thread thread;
    private volatile boolean running = true;

    public ErrorCheckerSink(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        throughputCounter = new AtomicLong(0);
        errorCounter = new AtomicInteger(0);
        Map<String, String> configMap = new HashMap<>();
        configMap.put("dataSchema", "string");
        dimRedisKvTextSchema = (DimRedisKvTextSchema) DimRedisSchema.getDimSchema(configMap);
        lastTime = System.currentTimeMillis();
        thread = new Thread(() -> {
            while (running) {
                long now = System.currentTimeMillis();
                if (now - lastTime >= 10_000) {
                    LOG.info("processed " + throughputCounter.get() + " msg(s) in last 10s.");
                    lastTime = now;
                    throughputCounter.set(0);
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {

                }
            }
        });
        thread.start();
    }

    @Override
    public void close() throws Exception {
        running = false;
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        long count = throughputCounter.incrementAndGet();
        if (count % 500 == 0) {
            Map<String, String> map = dimRedisKvTextSchema.parseInput(value);
            String key = map.get(keyColumn);
            String ids = map.get("id");
            int id = Integer.parseInt(ids);
            String update_time = map.get("update_time");
            if (id < 100) {

            }
        }
    }
}
