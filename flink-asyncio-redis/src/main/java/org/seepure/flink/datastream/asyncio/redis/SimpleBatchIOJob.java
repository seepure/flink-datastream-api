package org.seepure.flink.datastream.asyncio.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.seepure.flink.datastream.asyncio.redis.SimpleAsyncIOJob.SimpleRedisAsyncFunction;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisHashSchema;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisSchema;
import org.seepure.flink.datastream.asyncio.redis.config.JoinRule;
import org.seepure.flink.datastream.asyncio.redis.config.RedissonConfig;
import org.seepure.flink.datastream.asyncio.redis.config.SourceSchema;
import org.seepure.flink.datastream.asyncio.redis.sink.SimpleSinkFunction;
import org.seepure.flink.datastream.asyncio.redis.source.SelfRandomSource;
import org.seepure.flink.datastream.asyncio.redis.util.ArgUtil;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleBatchIOJob {

    public static void main(String[] args) throws Exception {
        String defaultRedisStringJoinArg =
                "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                        + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.kv_text;dim.schema.content={};joinRule.rightFields=tp_%s";
        String defaultRedisHashJoinArg =
                "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                        + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.hash;dim.schema.content={};joinRule.rightFields=th_%s";
        String arg = args != null && args.length >= 1 ? args[0] : defaultRedisStringJoinArg;
        //"redis.mode=cluster;redis.nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001";
        Map<String, String> configMap = ArgUtil.getArgMapFromArgs(arg);
        configMap.put("redis.nodes", "redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001");
        //configMap.put("redis.nodes", "redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001");
        ParameterTool params = ParameterTool.fromMap(configMap);
        StreamExecutionEnvironment env = getEnv(params);
        DataStream<String> in = env.addSource(new SelfRandomSource("mykey", 10, 1000));
        SingleOutputStreamOperator<String> stream = in.flatMap(new SimpleRedisBatchFlatMap(configMap));
        stream.addSink(new SimpleSinkFunction());
        env.execute();
    }

    private static StreamExecutionEnvironment getEnv(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
        return env;
    }

    public static class SimpleRedisBatchFlatMap extends RichFlatMapFunction<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(SimpleRedisBatchFlatMap.class);
        private static final AtomicInteger REF_COUNTER = new AtomicInteger(0);
        private static volatile transient RedissonClient client;
        private transient Object collectorLock;
        private volatile boolean running = true;
        private Map<String, String> configMap;
        private SourceSchema sourceSchema;
        private DimRedisSchema dimRedisSchema;
        private JoinRule joinRule;
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
            collectorLock = new Object();
            REF_COUNTER.getAndIncrement();
            sourceSchema = SourceSchema.getSourceSchema(configMap);
            dimRedisSchema = DimRedisSchema.getDimSchema(configMap);
            joinRule = JoinRule.parseJoinRule(configMap);
            batchSize = Integer.parseInt(configMap.getOrDefault("batchSize", "10"));
            minBatchTime = Integer.parseInt(configMap.getOrDefault("minBatchTime", "500"));
            buffer = new LinkedBlockingQueue<>(batchSize + batchSize >> 1);
            if (client == null) {
                synchronized (SimpleRedisAsyncFunction.class) {
                    if (client == null) {
                        Config config = RedissonConfig.getRedissonConfig(configMap);
                        client = Redisson.create(config);
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
                        Thread.sleep(50);
                    } catch (Exception e) {

                    }
                }
            });

        }

        @Override
        public void close() throws Exception {
            running = false;
            while (!buffer.isEmpty()) {
                List<BufferEntry> entries = new ArrayList<>(batchSize);
                buffer.drainTo(entries, batchSize);
                doBufferBatch(entries);
            }

            int refCount = REF_COUNTER.decrementAndGet();
            if (refCount <= 0 && client != null) {
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
//            if (cached) {
//                synchronized (collectorLock) {
//                    out.collect(result from cache);
//                }
//                return;
//            }

            //2. query redis
            BufferEntry bufferEntry = new BufferEntry(redisKey, input, source);
            buffer.put(bufferEntry);
        }

        private void doBufferBatch(List<BufferEntry> entries) {
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
                        ex.printStackTrace();
                    } else if (res != null) {
                        Map<String, String> map = dimRedisSchema.parseInput(String.valueOf(res));
                        //todo 根据JoinRule决定要输出哪些字段, 当前把所有的字段都输出
                        bufferEntry.getSource().putAll(map);
                        results.add(bufferEntry.getSource());
                    }
                });
            }
            batch.execute();
            synchronized (collectorLock) {
                for (Map<String, String> map : results) {
                    //todo 根据JoinRule 来决定输出格式
                    out.collect(ArgUtil.mapToBeaconKV(map));
                }
            }
        }

        private void doHashJoin(List<BufferEntry> entries) {
            Collector<String> out = this.out;
            RBatch batch = client.createBatch();
            List<Map<String, String>> results = new ArrayList<>(entries.size());
            for (BufferEntry bufferEntry : entries) {
                RFuture<Map<Object, Object>> rFuture = batch.getMap(bufferEntry.getRedisKey()).readAllMapAsync();
                rFuture.whenComplete((res, ex) -> {
                    if (res != null) {
                        Map<String, String> map = dimRedisSchema.parseInput(res);
                        //todo 根据JoinRule决定要输出哪些字段, 当前把所有的字段都输出
                        bufferEntry.getSource().putAll(map);
                        results.add(bufferEntry.getSource());
                    }
                });
            }
            batch.execute();
            synchronized (collectorLock) {
                for (Map<String, String> map : results) {
                    //todo 根据JoinRule 来决定输出格式
                    out.collect(ArgUtil.mapToBeaconKV(map));
                }
            }
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
