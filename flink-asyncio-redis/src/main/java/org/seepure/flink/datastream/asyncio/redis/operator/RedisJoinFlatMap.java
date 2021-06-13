package org.seepure.flink.datastream.asyncio.redis.operator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.seepure.flink.datastream.asyncio.redis.config.CachePolicy;
import org.seepure.flink.datastream.asyncio.redis.config.JoinRule;
import org.seepure.flink.datastream.asyncio.redis.config.RedisHashSchemaExecutor;
import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig;
import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig.TableSchema;
import org.seepure.flink.datastream.asyncio.redis.config.RedissonConfig;
import org.seepure.flink.datastream.asyncio.redis.config.TableSchemaExecutor;
import org.seepure.flink.datastream.asyncio.redis.config.TableSchemaExecutorBuilder;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisJoinFlatMap extends RichFlatMapFunction<byte[], String> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisJoinFlatMap.class);
    private static final AtomicInteger REF_COUNTER = new AtomicInteger(0);
    private static transient volatile RedissonClient client;
    private static transient volatile Cache<String, Object> cache;
    private transient ReentrantLock collectorLock;
    private volatile boolean running = true;
    private TableSchemaExecutor sourceSchema;
    private TableSchemaExecutor dimRedisSchema;
    private JoinRule joinRule;
    private CachePolicy cachePolicy;
    private int batchSize;
    private long minBatchTime;
    private String outputCharset;
    private volatile long lastDoBufferTime;
    private transient BlockingQueue<BufferEntry> buffer;
    private transient ExecutorService threadPool; // = Executors.newFixedThreadPool(1);
    private transient Collector<String> out;
    private String configString;

    public RedisJoinFlatMap(String configString) {
        this.configString = configString;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //1. get configMap from parameters
        String json = configString;
        LOG.info("config json: " + json);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        RedisJoinConfig redisJoinConfig = null;
        try {
            redisJoinConfig = objectMapper.readValue(json, RedisJoinConfig.class);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalArgumentException(
                    "objectMapper.readValue(json, RedisJoinConfig.class) error! config_json: " + json, e);
        }
        if (redisJoinConfig == null) {
            throw new IllegalArgumentException(
                    "open RedisJoinFlatMap failed. get empty redisJoinConfig! config_json: " + json);
        }

        LOG.info("redisJoinConfig: " + redisJoinConfig.toString());
        collectorLock = new ReentrantLock();
        REF_COUNTER.getAndIncrement();
        try {
            AssertUtil.assertTrue(CollectionUtils.isNotEmpty(redisJoinConfig.getSrcTableSchemas()),
                    "redisJoinConfig.getSrcTableSchemas() is empty!");
            for (TableSchema tableSchema : redisJoinConfig.getSrcTableSchemas()) {
                if ("dim_table".equalsIgnoreCase(tableSchema.getTableType())) {
                    dimRedisSchema = TableSchemaExecutorBuilder.build(tableSchema);
                } else {
                    sourceSchema = TableSchemaExecutorBuilder.build(tableSchema);
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "parse sourceSchema or dimRedisSchema error! " + e.getMessage() + ". config_json: " + json, e);
        }
        if (dimRedisSchema == null) {
            throw new IllegalArgumentException("get empty dimRedisSchema! please check srcTableSchemas. config_json: " + json);
        }
        if (sourceSchema == null) {
            throw new IllegalArgumentException("get empty sourceSchema! please check srcTableSchemas. config_json: " + json);
        }
        joinRule = JoinRule.parseJoinRule(redisJoinConfig);
        batchSize = redisJoinConfig.getBatchSize() == null || redisJoinConfig.getBatchSize() < 1000 ? 1000
                : redisJoinConfig.getBatchSize();
        minBatchTime = redisJoinConfig.getMinBatchTime() == null || redisJoinConfig.getMinBatchTime() < 1000 ? 1000
                : redisJoinConfig.getBatchSize();
        outputCharset = "UTF-8";    //应该在输出tableSchema中定义的
        buffer = new LinkedBlockingQueue<>(batchSize + batchSize >> 1);
        if (client == null || client.isShutdown()) {
            synchronized (this.getClass()) {
                if (client == null || client.isShutdown()) {
                    Config config = RedissonConfig.getRedissonConfig(redisJoinConfig);
                    client = Redisson.create(config);
                }
            }
        }
        cachePolicy = CachePolicy.getCachePolicy(redisJoinConfig);
        if (cachePolicy != null && cache == null) {
            synchronized (this.getClass()) {
                if (cache == null) {
                    LOG.info("CachePolicy: " + cachePolicy.toString());
                    @NonNull Caffeine<Object, Object> builder = Caffeine.newBuilder();
                    builder.maximumSize(cachePolicy.getSize());
                    if (cachePolicy.getExpireAfterWrite() > 0) {
                        builder.expireAfterWrite(cachePolicy.getExpireAfterWrite(), TimeUnit.SECONDS);
                    }
                    cache = builder.recordStats().build();
                    Thread cacheMonitorThread = createCacheMonitorThread(cache);
                    cacheMonitorThread.start();
                }
            }
        }
        lastDoBufferTime = System.currentTimeMillis();
        threadPool = Executors.newFixedThreadPool(1);
        threadPool.execute(() -> {
            while (running) {
                try {
                    long now = System.currentTimeMillis();
                    if (buffer.size() >= batchSize || now - lastDoBufferTime >= minBatchTime) {
                        lastDoBufferTime = now;
                        List<BufferEntry> entries = new ArrayList<>(batchSize);
                        buffer.drainTo(entries, batchSize);
                        doBufferBatch(entries);
                    }
                    Thread.sleep(5);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        running = false;
        threadPool.shutdown();
        while (!buffer.isEmpty()) {
            List<BufferEntry> entries = new ArrayList<>(batchSize);
            buffer.drainTo(entries, batchSize);
            try {
                doBufferBatch(entries);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        int refCount = REF_COUNTER.decrementAndGet();
        if (refCount <= 0 && client != null) {
            client.shutdown();
        }
    }

    @Override
    public void flatMap(byte[] value, Collector<String> out) throws Exception {
        if (value == null || value.length < 1) {
            return;
        }
        Map<String, String> source = sourceSchema.parseBytes(value);
        if (source == null || source.isEmpty()) {
            return;
        }
        this.out = out;

        String keyExpression = joinRule.getRightFields().get(0);
        String sourceJoinColumnName = joinRule.getLeftFields().get(0);
        String sourceJoinColumnValue = source.get(sourceJoinColumnName);
        String redisKey = String.format(keyExpression, sourceJoinColumnValue);

        //1. deal with cache
        if (cache != null) {
            Object cachedResult = cache.getIfPresent(redisKey);
            if (cachedResult != null) {
                if (JoinRule.INNER_JOIN_TYPE.equalsIgnoreCase(joinRule.getType())
                        && isNullResult(cachedResult)) {
                    return;
                }
                source.putAll(dimRedisSchema.parseInput(cachedResult));
                String result = serialize(source, value);
                collectorLock.lock();
                try {
                    out.collect(result);
                } catch (Exception be) {
                    throw be;
                } finally {
                    collectorLock.unlock();
                }
                return;
            }
        }

        //2. buffering and then query redis
        BufferEntry bufferEntry = new BufferEntry(redisKey, value, source);
        try {
            buffer.put(bufferEntry);
        } catch (InterruptedException e) {
            LOG.error("put data to " + getClass().getSimpleName() + " queue with error!", e);
        }

    }

    protected void doBufferBatch(List<BufferEntry> entries) throws Exception {
        if (CollectionUtils.isEmpty(entries)) {
            return;
        }
        if (dimRedisSchema instanceof RedisHashSchemaExecutor) {
            doHashJoin(entries);
        } else {
            doStringJoin(entries);
        }
    }

    private void doStringJoin(List<BufferEntry> entries) throws Exception {
        RBatch batch = client.createBatch();
        List<Map<String, String>> results = new ArrayList<>(entries.size());
        for (BufferEntry bufferEntry : entries) {
            RFuture<Object> rFuture = batch.getBucket(bufferEntry.getRedisKey()).getAsync();
            rFuture.whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.error(ex.getMessage(), ex);
                }
                if (cache != null) {
                    if (cachePolicy.isNullable()) {
                        cache.put(bufferEntry.getRedisKey(), res == null ? "" : res);  //cache nullable
                    } else if (res != null) {
                        cache.put(bufferEntry.getRedisKey(), res);
                    }
                }
                if (res != null || !JoinRule.INNER_JOIN_TYPE.equalsIgnoreCase(joinRule.getType())) {
                    Map<String, String> map = dimRedisSchema.parseInput(res);
                    //todo 根据JoinRule决定要输出哪些字段, 当前把所有的字段都输出
                    bufferEntry.getSource().putAll(map);
                    results.add(bufferEntry.getSource());
                }

            });
        }
        batch.execute();
        collectorLock.lock();
        try {
            for (Map<String, String> map : results) {
                //todo 根据JoinRule 来决定输出格式
                out.collect(serialize(map, (byte[]) entries.get(0).getInput()));
            }
        } catch (Exception be) {
            throw be;
        } finally {
            collectorLock.unlock();
        }
    }

    private void doHashJoin(List<BufferEntry> entries) throws Exception {
        RBatch batch = client.createBatch();
        List<Map<String, String>> results = new ArrayList<>(entries.size());
        for (BufferEntry bufferEntry : entries) {
            RFuture<Map<Object, Object>> rFuture = batch.getMap(bufferEntry.getRedisKey()).readAllMapAsync();
            rFuture.whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.error(ex.getMessage(), ex);
                }
                if (cache != null) { //cache nullable
                    if (cachePolicy.isNullable()) {
                        cache.put(bufferEntry.getRedisKey(),
                                res == null || res.isEmpty() ? Collections.EMPTY_MAP : res);
                    } else if (res != null && !res.isEmpty()) {
                        cache.put(bufferEntry.getRedisKey(), res);
                    }
                }
                if ((res != null && !res.isEmpty()) || !JoinRule.INNER_JOIN_TYPE.equalsIgnoreCase(joinRule.getType())) {
                    Map<String, String> map = dimRedisSchema.parseInput(res);
                    //todo 根据JoinRule决定要输出哪些字段, 当前把所有的字段都输出
                    bufferEntry.getSource().putAll(map);
                    results.add(bufferEntry.getSource());
                }
            });
        }
        batch.execute();
        collectorLock.lock();
        try {
            for (Map<String, String> map : results) {
                //todo 根据JoinRule 来决定输出格式
                out.collect(serialize(map, (byte[]) entries.get(0).getInput()));
            }
        } catch (Exception be) {
            throw be;
        } finally {
            collectorLock.unlock();
        }
    }

    private String serialize(Map<String, String> map, byte[] inputMsg) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            if (i < map.size() - 1) {
                sb.append("|");
            }
            i++;
        }
        String s = sb.toString();
        return s;
    }

    private boolean isNullResult(Object cacheResult) {
        if (cacheResult == null) {
            return true;
        }
        if (cacheResult instanceof String) {
            return StringUtils.isBlank((String) cacheResult);
        } else if (cacheResult instanceof Map) {
            return ((Map) cacheResult).isEmpty();
        }
        return true;
    }

    private Thread createCacheMonitorThread(Cache<String, Object> cache) {
        AssertUtil.assertTrue(cache != null, "cache is null!");
        return new Thread(() -> {
            while (running) {
                try {
                    CacheStats stats = cache.stats();
                    //todo 上报监控
                    LOG.info(stats.toString());
                    Thread.sleep(30_000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static class BufferEntry {

        private String redisKey;
        private Object input;
        private Map<String, String> source;

        public BufferEntry() {
        }

        public BufferEntry(String redisKey, Object input, Map<String, String> source) {
            this.redisKey = redisKey;
            this.input = input;
            this.source = source;
        }

        public String getRedisKey() {
            return redisKey;
        }

        public void setRedisKey(String redisKey) {
            this.redisKey = redisKey;
        }

        public Object getInput() {
            return input;
        }

        public void setInput(Object input) {
            this.input = input;
        }

        public Map<String, String> getSource() {
            return source;
        }

        public void setSource(Map<String, String> source) {
            this.source = source;
        }
    }
}
