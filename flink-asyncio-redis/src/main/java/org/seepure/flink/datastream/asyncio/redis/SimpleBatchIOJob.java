package org.seepure.flink.datastream.asyncio.redis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.seepure.flink.datastream.asyncio.redis.config.CachePolicy;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisHashSchema;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisSchema;
import org.seepure.flink.datastream.asyncio.redis.config.JoinRule;
import org.seepure.flink.datastream.asyncio.redis.config.RedissonConfig;
import org.seepure.flink.datastream.asyncio.redis.config.SourceSchema;
import org.seepure.flink.datastream.asyncio.redis.sink.PerformanceCountSink;
import org.seepure.flink.datastream.asyncio.redis.sink.SimpleSinkFunction;
import org.seepure.flink.datastream.asyncio.redis.source.PressureRandomSource;
import org.seepure.flink.datastream.asyncio.redis.source.SelfRandomKVSource;
import org.seepure.flink.datastream.asyncio.redis.source.SelfRandomTextSource;
import org.seepure.flink.datastream.asyncio.redis.util.ArgUtil;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;
import org.seepure.flink.datastream.asyncio.redis.util.MonitorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleBatchIOJob {

    public static void main(String[] args) throws Exception {
        String defaultRedisStringJoinArg =
                "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                        + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.kv_text;dim.schema.content={};joinRule.rightFields=tp_%s";
//        String defaultRedisHashJoinArg =
//                "redis.mode=cluster;redis.nodes=redis://192.168.234.139:7000,redis://192.168.234.139:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001"
//                        + ";source.schema.type=MQ_TEXT;source.schema.content={\"sourceType\":\"ATTA\",\"contentType\":\"MQ_TEXT\",\"encoding\":\"UTF-8\",\"separator\":\"|\",\"peekQps\":1,\"maxStorageAday\":3,\"schema\":[{\"fieldKey\":\"userId\",\"fieldName\":\"用户id\",\"fieldType\":1,\"fieldIndex\":0},{\"fieldKey\":\"age\",\"fieldName\":\"年龄\",\"fieldType\":1,\"fieldIndex\":1},{\"fieldKey\":\"country\",\"fieldName\":\"国家\",\"fieldType\":1,\"fieldIndex\":2}],\"metadataId\":100};"
//                        + "dim.schema.type=redis.hash;dim.schema.content={};joinRule.type=left_join;joinRule.rightFields=bh_%s;joinRule.leftFields=userId;cachePolicy.type=local;cachePolicy.expireAfterWrite=50;cachePolicy.size=200";
        String defaultRedisHashJoinArg = "redis.mode=cluster;redis.nodes=9.146.159.128:6379;source.schema.type=MQ_TEXT;source.schema.content={\"sourceType\":\"ATTA\",\"contentType\":\"MQ_TEXT\",\"encoding\":\"UTF-8\",\"separator\":\"|\",\"peekQps\":1,\"maxStorageAday\":3,\"schema\":[{\"fieldKey\":\"userId\",\"fieldName\":\"用户id\",\"fieldType\":1,\"fieldIndex\":0},{\"fieldKey\":\"age\",\"fieldName\":\"年龄\",\"fieldType\":1,\"fieldIndex\":1},{\"fieldKey\":\"country\",\"fieldName\":\"国家\",\"fieldType\":1,\"fieldIndex\":2}],\"metadataId\":100};dim.schema.type=redis.hash;dim.schema.content={};joinRule.rightFields=bh_%s;joinRule.leftFields=userId";
        String arg = args != null && args.length >= 1 ? args[0] : defaultRedisHashJoinArg;
        //"redis.mode=cluster;redis.nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001";
        Map<String, String> configMap = ArgUtil.getArgMapFromArgs(arg);
        //configMap.put("redis.nodes", "redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001");
        //configMap.put("redis.nodes", "redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001");
        ParameterTool params = ParameterTool.fromMap(configMap);
        StreamExecutionEnvironment env = getEnv(params);
        DataStream<String> in = env.addSource(new PressureRandomSource("mykey", 1_000_000, 1000));
        SingleOutputStreamOperator<String> stream = in.flatMap(new SimpleRedisBatchFlatMap(configMap));
        stream.addSink(new PerformanceCountSink());
        env.execute();
    }

    private static StreamExecutionEnvironment getEnv(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().enableObjectReuse();
        int parallel = Integer.parseInt(params.get("parallel", "1"));
        env.setParallelism(parallel);
        return env;
    }

    public static class SimpleRedisBatchFlatMap extends RichFlatMapFunction<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(SimpleRedisBatchFlatMap.class);
        private static final AtomicInteger REF_COUNTER = new AtomicInteger(0);
        private static volatile transient RedissonClient client;
        private static volatile transient Cache<String, Object> cache;
        private transient ReentrantLock collectorLock;
        private volatile boolean running = true;
        private Map<String, String> configMap;
        private SourceSchema sourceSchema;
        private DimRedisSchema dimRedisSchema;
        private JoinRule joinRule;
        private CachePolicy cachePolicy;
        private int batchSize;
        private long minBatchTime;
        private volatile long lastDoBufferTime;
        private transient BlockingQueue<BufferEntry> buffer;
        private transient ExecutorService threadPool; // = Executors.newFixedThreadPool(1);
        private transient Collector<String> out;

        public SimpleRedisBatchFlatMap(Map<String, String> configMap) {
            this.configMap = configMap;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            AssertUtil.assertTrue(configMap != null, "empty configMap!");
            LOG.info("configMap: " + configMap.toString());
            collectorLock = new ReentrantLock();
            REF_COUNTER.getAndIncrement();
            sourceSchema = SourceSchema.getSourceSchema(configMap);
            dimRedisSchema = DimRedisSchema.getDimSchema(configMap);
            joinRule = JoinRule.parseJoinRule(configMap);
            batchSize = Integer.parseInt(configMap.getOrDefault("batchSize", "2000"));
            minBatchTime = Integer.parseInt(configMap.getOrDefault("minBatchTime", "1000"));
            buffer = new LinkedBlockingQueue<>(batchSize + batchSize >> 1);
            if (client == null) {
                synchronized (this.getClass()) {
                    if (client == null) {
                        Config config = RedissonConfig.getRedissonConfig(configMap);
                        client = Redisson.create(config);
                    }
                }
            }
            cachePolicy = CachePolicy.getCachePolicy(configMap);
            if (cachePolicy != null && cache == null) {
                synchronized (this.getClass()) {
                    if (cache == null) {
                        cache = Caffeine.newBuilder().maximumSize(cachePolicy.getSize())
                                .expireAfterWrite(cachePolicy.getExpireAfterWrite(), TimeUnit.SECONDS).recordStats()
                                .build();
                        Thread cacheMonitorThread = MonitorUtil.createCacheMonitorThread(cache);
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
                doBufferBatch(entries);
            }

            int refCount = REF_COUNTER.decrementAndGet();
            if (refCount <= 0 && client != null) {
                MonitorUtil.stop();
                client.shutdown();
            }
        }

        @Override
        public void flatMap(String input, Collector<String> out) throws Exception {
            Map<String, String> source = sourceSchema.parseInput(input);
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
                    collectorLock.lock();
                    try {
                        out.collect(ArgUtil.mapToBeaconKV(source));
                    } finally {
                        collectorLock.unlock();
                    }
                    return;
                }
            }

            //2. buffering and then query redis
            BufferEntry bufferEntry = new BufferEntry(redisKey, input, source);
            buffer.put(bufferEntry);
        }

        protected void doBufferBatch(List<BufferEntry> entries) {
            if (CollectionUtils.isEmpty(entries)) {
                return;
            }
            if (dimRedisSchema instanceof DimRedisHashSchema) {
                doHashJoin(entries);
            } else {
                doStringJoin(entries);
            }
        }

        private void doStringJoin(List<BufferEntry> entries) {
            Collector<String> out = this.out;
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
                    out.collect(ArgUtil.mapToBeaconKV(map));
                }
            } finally {
                collectorLock.unlock();
            }
        }

        private void doHashJoin(List<BufferEntry> entries) {
            Collector<String> out = this.out;
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
                    out.collect(ArgUtil.mapToBeaconKV(map));
                }
            } finally {
                collectorLock.unlock();
            }
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

        private static class BufferEntry {

            private String redisKey;
            private String input;
            private Map<String, String> source;

            public BufferEntry() {
            }

            public BufferEntry(String redisKey, String input, Map<String, String> source) {
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

            public String getInput() {
                return input;
            }

            public void setInput(String input) {
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

}
