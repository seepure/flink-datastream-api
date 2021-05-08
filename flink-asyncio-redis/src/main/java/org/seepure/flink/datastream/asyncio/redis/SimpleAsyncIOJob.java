package org.seepure.flink.datastream.asyncio.redis;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisKvTextSchema;
import org.seepure.flink.datastream.asyncio.redis.config.DimRedisSchema;
import org.seepure.flink.datastream.asyncio.redis.config.JoinRule;
import org.seepure.flink.datastream.asyncio.redis.config.RedissonConfig;
import org.seepure.flink.datastream.asyncio.redis.config.SourceSchema;
import org.seepure.flink.datastream.asyncio.redis.sink.SimpleSinkFunction;
import org.seepure.flink.datastream.asyncio.redis.source.SelfRandomKVSource;
import org.seepure.flink.datastream.asyncio.redis.util.ArgUtil;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SimpleAsyncIOJob {

    public static void main(String[] args) throws Exception {
        String defaultRedisStringJoinArg = "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.kv_text;dim.schema.content={}";
        String defaultRedisHashJoinArg = "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.hash;dim.schema.content={};joinRule.rightFields=th_%s";
        String arg = args != null && args.length >= 1 ? args[0] : defaultRedisHashJoinArg;
                //"redis.mode=cluster;redis.nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001";
        Map<String, String> configMap = ArgUtil.getArgMapFromArgs(arg);
        //configMap.put("redis.nodes", "192.168.234.137:7000,192.168.234.137:7001,192.168.234.138:7000,192.168.234.138:7001,192.168.234.134:7000,192.168.234.134:7001");
        ParameterTool params = ParameterTool.fromMap(configMap);
        long timeout = 1;
        StreamExecutionEnvironment env = getEnv(params);
        DataStream<String> in = env.addSource(new SelfRandomKVSource("mykey", 10, 1000));
        SingleOutputStreamOperator<String> stream = AsyncDataStream
                .unorderedWait(in, new SimpleRedisAsyncFunction(), timeout * 2, TimeUnit.SECONDS, 20);
        //stream.print();
        stream.addSink(new SimpleSinkFunction());

          //The operator for AsyncFunction (AsyncWaitOperator) must currently be at the head of operator chains for consistency reasons
        //For the reasons given in issue FLINK-13063, we currently must break operator chains for the AsyncWaitOperator to prevent potential consistency problems.
        // This is a change to the previous behavior that supported chaining.
        // User that require the old behavior and accept potential violations of the consistency guarantees can instantiate
        // and add the AsyncWaitOperator manually to the job graph and set the chaining strategy back to chaining
        // via AsyncWaitOperator#setChainingStrategy(ChainingStrategy.ALWAYS)
        // the code is show below
//        SimpleRedisAsyncFunction func = new SimpleRedisAsyncFunction();
//        TypeInformation<String> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
//                func,
//                AsyncFunction.class,
//                0,
//                1,
//                new int[]{1, 0},
//                in.getType(),
//                Utils.getCallLocationName(),
//                true);
//
//        // create transform
//        AsyncWaitOperator<String, String> operator = new AsyncWaitOperator<>(
//                in.getExecutionEnvironment().clean(func),
//                timeout,
//                10,
//                OutputMode.UNORDERED);
//        operator.setChainingStrategy(ChainingStrategy.ALWAYS);
//        SingleOutputStreamOperator<String> asyncWaitOperator = in
//                .transform("async wait operator", outTypeInfo, operator);
//        asyncWaitOperator.print();
        env.execute();
    }

    private static StreamExecutionEnvironment getEnv(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
        return env;
    }

    public static class SimpleRedisAsyncFunction extends RichAsyncFunction<String, String> {

        private static volatile transient RedissonClient client;
        private long timeout;
        private Map<String, String> configMap;
        private SourceSchema sourceSchema;
        private DimRedisSchema dimRedisSchema;
        private JoinRule joinRule;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig()
                    .getGlobalJobParameters();
            configMap = globalParams.toMap();
            timeout = Long.valueOf(configMap.getOrDefault("redis.timeout", "10"));
            sourceSchema = SourceSchema.getSourceSchema(configMap);
            dimRedisSchema = DimRedisSchema.getDimSchema(configMap);
            joinRule = JoinRule.parseJoinRule(configMap);
            if (client == null) {
                synchronized (SimpleRedisAsyncFunction.class) {
                    if (client == null) {
                        Config config = RedissonConfig.getRedissonConfig(configMap);
                        client = Redisson.create(config);
                    }
                }
            }

        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
            Map<String, String> source = sourceSchema.parseInput(input);
            if (source == null || source.isEmpty()) {
                return;
            }
            String keyExpression = joinRule.getRightFields().get(0);
            String sourceJoinColumnName = joinRule.getLeftFields().get(0);
            String sourceJoinColumnValue = source.get(sourceJoinColumnName);
            String redisKey = String.format(keyExpression, sourceJoinColumnValue);
            //1. deal with cache

            //2. query redis
            if (dimRedisSchema instanceof DimRedisKvTextSchema) {
                doStringJoin(redisKey, input, source, resultFuture);
            } else {
                doHashJoin(redisKey, input, source, resultFuture);
            }
        }

        private void doStringJoin(String key, String input, Map<String, String> source, ResultFuture<String> resultFuture) {
            RBucket<Object> bucket = client.getBucket(key);

            RFuture<Object> objectRFuture = bucket.getAsync();

            objectRFuture.whenComplete((res, exception) -> {
                if (exception != null) {
                    //这里一定要保留，否则会阻塞
                    exception.printStackTrace();    //todo 抽样打印日志
                    resultFuture.complete(Collections.singletonList(""));
                } else if (res == null) {
                    //这里一定要保留，否则会阻塞
                    resultFuture.complete(Collections.singletonList(""));
                } else {
                    int size = 0;
                    if (source != null) {
                        size += source.size();
                    }
                    Map<String, String> resMap = dimRedisSchema.parseInput(res);
                    size += resMap.size();
                    Map<String, String> finalMap = new LinkedHashMap<>(size);
                    if (source != null) {
                        finalMap.putAll(source);
                    }
                    finalMap.putAll(resMap);
                    resultFuture.complete(Collections.singletonList(ArgUtil.mapToBeaconKV(finalMap)));
                }

            });
        }

        private void doHashJoin(String key, String input, Map<String, String> source, ResultFuture<String> resultFuture) {
            RMap<String, String> rMap = client.getMap(key);
            RFuture<Set<Map.Entry<String, String>>> setRFuture = rMap.readAllEntrySetAsync();
            setRFuture.whenComplete((res, exception) -> {
                if (exception != null) {
                    //这里一定要保留，否则会阻塞
                    exception.printStackTrace();    //todo 抽样打印日志
                    resultFuture.complete(Collections.singletonList(""));
                } else if (res == null) {
                    //这里一定要保留，否则会阻塞
                    resultFuture.complete(Collections.singletonList(""));
                } else {
                    int size = res.size();
                    if (source != null) {
                        size += source.size();
                    }
                    Map<String, String> map = new LinkedHashMap<>(size);
                    if (source != null) {
                        map.putAll(source);
                    }
                    for (Map.Entry<String, String> entry : res) {
                        map.put(entry.getKey(), entry.getValue());
                    }
                    resultFuture.complete(Collections.singletonList(ArgUtil.mapToBeaconKV(map)));
                }

            });
        }

        @Override
        public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {

        }
    }
}
