package org.seepure.flink.datastream.asyncio.redis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
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
import org.seepure.flink.datastream.asyncio.redis.source.SelfRandomTextSource;
import org.seepure.flink.datastream.asyncio.redis.util.ArgUtil;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;
import org.seepure.flink.datastream.asyncio.redis.util.MonitorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFlatMapJob {

    public static void main(String[] args) throws Exception {
        String defaultRedisStringJoinArg =
                "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                        + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.kv_text;dim.schema.content={};joinRule.rightFields=tp_%s;cachePolicy.type=local;cachePolicy.expireAfterWrite=20;cachePolicy.size=200";
//        String defaultRedisHashJoinArg =
//                "redis.mode=cluster;redis.nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001"
//                        + ";source.schema.type=MQ_TEXT;source.schema.content={\"sourceType\":\"ATTA\",\"contentType\":\"MQ_TEXT\",\"encoding\":\"UTF-8\",\"separator\":\"|\",\"peekQps\":1,\"maxStorageAday\":3,\"schema\":[{\"fieldKey\":\"userId\",\"fieldName\":\"??????id\",\"fieldType\":1,\"fieldIndex\":0},{\"fieldKey\":\"age\",\"fieldName\":\"??????\",\"fieldType\":1,\"fieldIndex\":1},{\"fieldKey\":\"country\",\"fieldName\":\"??????\",\"fieldType\":1,\"fieldIndex\":2}],\"metadataId\":100};"
//                        + "dim.schema.type=redis.hash;dim.schema.content={};joinRule.rightFields=kh_%s;joinRule.leftFields=userId;cachePolicy.type=local;cachePolicy.expireAfterWrite=20;cachePolicy.size=200";
        String defaultRedisHashJoinArg = "redis.mode=cluster;redis.nodes=9.146.159.128:6379;source.schema.type=MQ_TEXT;source.schema.content={\"sourceType\":\"ATTA\",\"contentType\":\"MQ_TEXT\",\"encoding\":\"UTF-8\",\"separator\":\"|\",\"peekQps\":1,\"maxStorageAday\":3,\"schema\":[{\"fieldKey\":\"userId\",\"fieldName\":\"??????id\",\"fieldType\":1,\"fieldIndex\":0},{\"fieldKey\":\"age\",\"fieldName\":\"??????\",\"fieldType\":1,\"fieldIndex\":1},{\"fieldKey\":\"country\",\"fieldName\":\"??????\",\"fieldType\":1,\"fieldIndex\":2}],\"metadataId\":100};dim.schema.type=redis.hash;dim.schema.content={};joinRule.rightFields=bh_%s;joinRule.leftFields=userId";
        String arg = args != null && args.length >= 1 ? args[0] : defaultRedisHashJoinArg;
        Map<String, String> configMap = ArgUtil.getArgMapFromArgs(arg);
        //configMap.put("redis.nodes", "redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001");
        //configMap.put("redis.nodes", "redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001");
        ParameterTool params = ParameterTool.fromMap(configMap);
        StreamExecutionEnvironment env = getEnv(params);
        DataStream<String> in = env.addSource(new PressureRandomSource("mykey", 1_000_000, 1000));
        SingleOutputStreamOperator<String> stream = in.flatMap(new SimpleRedisFlatMap(configMap));
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

    public static class SimpleRedisFlatMap extends RichFlatMapFunction<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(SimpleRedisFlatMap.class);
        private static final AtomicInteger REF_COUNTER = new AtomicInteger(0);
        private static volatile transient RedissonClient client;
        private static volatile transient Cache<String, Object> cache;
        private Map<String, String> configMap;
        private SourceSchema sourceSchema;
        private DimRedisSchema dimRedisSchema;
        private JoinRule joinRule;
        private CachePolicy cachePolicy;

        public SimpleRedisFlatMap(Map<String, String> configMap) {
            this.configMap = configMap;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            AssertUtil.assertTrue(configMap != null, "empty configMap!");
            LOG.info("configMap: " + configMap.toString());
            REF_COUNTER.getAndIncrement();
            sourceSchema = SourceSchema.getSourceSchema(configMap);
            dimRedisSchema = DimRedisSchema.getDimSchema(configMap);
            joinRule = JoinRule.parseJoinRule(configMap);
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
        }

        @Override
        public void close() throws Exception {
            int refCount = REF_COUNTER.decrementAndGet();
            if (refCount <= 0 && client != null) {
                MonitorUtil.stop();
                client.shutdown();
            }
            super.close();
        }

        @Override
        public void flatMap(String input, Collector<String> out) throws Exception {
            Map<String, String> source = sourceSchema.parseInput(input);
            if (source == null || source.isEmpty()) {
                return;
            }

            String keyExpression = joinRule.getRightFields().get(0);
            String sourceJoinColumnName = joinRule.getLeftFields().get(0);
            String sourceJoinColumnValue = source.get(sourceJoinColumnName);
            String redisKey = String.format(keyExpression, sourceJoinColumnValue);

            if (cache != null) {
                Object cachedResult = cache.getIfPresent(redisKey);
                if (cachedResult != null) {
                    source.putAll(dimRedisSchema.parseInput(cachedResult));
                    out.collect(ArgUtil.mapToBeaconKV(source));
                    return;
                }
            }

            if (dimRedisSchema instanceof DimRedisHashSchema) {
                RMap<Object, Object> rMap = client.getMap(redisKey);
                Map<Object, Object> readAllMap = rMap.readAllMap();
                if (cache != null) {
                    if (cachePolicy.isNullable()) {
                        cache.put(redisKey,
                                readAllMap == null || readAllMap.isEmpty() ? Collections.EMPTY_MAP : readAllMap);
                    } else if (readAllMap != null && !readAllMap.isEmpty()) {
                        cache.put(redisKey, readAllMap);
                    }
                }
                source.putAll(dimRedisSchema.parseInput(readAllMap));
            } else {
                RBucket<String> bucket = client.getBucket(redisKey);
                String o = bucket.get();
                if (cache != null) {
                    if (cachePolicy.isNullable()) {
                        cache.put(redisKey, o == null ? "" : o);  //cache nullable
                    } else if (o != null) {
                        cache.put(redisKey, o);
                    }
                }
                source.putAll(dimRedisSchema.parseInput(o));
            }
            out.collect(ArgUtil.mapToBeaconKV(source));
        }
    }

}
