package org.seepure.flink.datastream.asyncio.redis.sink;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisKvTextSchema;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorCheckerSink extends RichSinkFunction<String> {

    private static Logger LOG = LoggerFactory.getLogger(ErrorCheckerSink.class);
    private AtomicLong throughputCounter;
    private AtomicInteger errorCounter;
    private DimRedisKvTextSchema dimRedisKvTextSchema;
    private String keyColumn;
    private int bound;
    private int cacheExpireSecond;
    private long lastTime;
    private Thread thread;
    private SimpleDateFormat sdf;
    private volatile boolean running = true;

    public ErrorCheckerSink(String keyColumn, int bound, int cacheExpireSecond) {
        this.keyColumn = keyColumn;
        this.bound = bound;
        this.cacheExpireSecond = cacheExpireSecond;
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
        sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        thread = new Thread(() -> {
            while (running) {
                long now = System.currentTimeMillis();
                if (now - lastTime >= 10_000) {
                    LOG.info("processed " + throughputCounter.get() + " msg(s) in last 10s.");
                    LOG.info("errorCount: " + errorCounter.get() + " in last 10s. ");
                    lastTime = now;
                    throughputCounter.set(0);
                    errorCounter.set(0);
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
        if (count % 1000 == 0) {
            Map<String, String> map = dimRedisKvTextSchema.parseInput(value);
            String key = map.get(keyColumn);
            String ids = map.get("id");
            if (!Objects.equals(key, ids)) {
                errorCounter.incrementAndGet();
                LOG.error(String.format("checkError for key: %s and id: %s", key, ids));
                return;
            }
            int id = Integer.parseInt(ids);
            String update_time = map.get("update_time");
            long now = System.currentTimeMillis();
            Date parse = sdf.parse(update_time);
            long update_timestamp = parse.getTime();
            if (id < bound / 100) {
                if ((now - update_timestamp) - 6000L > cacheExpireSecond * 1000L) {
                    errorCounter.incrementAndGet();
                    LOG.error("checkError for update_timestamp! " + map.toString());
                }
            } else {
                if ((now - update_timestamp) - 12_000L > 900_000L) {
                    errorCounter.incrementAndGet();
                    LOG.error("checkError for update_timestamp! " + map.toString());
                }
            }
        }
    }
}
