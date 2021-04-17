package org.seepure.flink.datastream.asyncio.redis;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.config.SingleServerConfig;
import org.seepure.flink.datastream.asyncio.redis.sink.SimpleSinkFunction;
import org.seepure.flink.datastream.asyncio.redis.source.SelfRandomSource;

public class SimpleAsyncIOJob {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        long timeout = params.getInt("redis.timeout", 1);
        StreamExecutionEnvironment env = getEnv(params);
        DataStream<String> in = env.addSource(new SelfRandomSource("mykey", 10, 1000));
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

        private static transient RedissonClient client;
        private long timeout;
        private Map<String, String> configMap;
        private SourceSchema sourceSchema;
        private JoinRule joinRule;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig()
                    .getGlobalJobParameters();
            configMap = globalParams.toMap();
            timeout = Long.valueOf(configMap.getOrDefault("redis.timeout", "10"));
            sourceSchema = SourceSchema.parseSourceSchema(configMap);
            joinRule = JoinRule.parseJoinRule(configMap);
            if (client == null) {
                synchronized (SimpleRedisAsyncFunction.class) {
                    if (client == null) {
                        Config config = buildRedissonConfig(configMap);
                        client = Redisson.create(config);
                    }
                }
            }

        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
            Map<String, String> source = parseInput(input);
            if (source == null || source.isEmpty()) {
                return;
            }
            String redisKeyPrefix = joinRule.getRightFields().get(0);
            String sourceKeyName = joinRule.getLeftFields().get(0);
            String sourceKeyValue = source.get(sourceKeyName);
            //1. deal with cache

            //2. query redis
            RBucket<Object> bucket = client.getBucket(redisKeyPrefix + sourceKeyValue);

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
                    resultFuture.complete(Collections.singletonList(input + "|" + "joinResult=" + res));
                }

            });

        }

        @Override
        public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {

        }

        private Config buildRedissonConfig(Map<String, String> configMap) {
            String mode = configMap.getOrDefault("redis.mode", "").toLowerCase();
            String nodes = configMap.getOrDefault("redis.nodes", "redis://192.168.234.137:6379");
            String password = configMap.get("redis.auth");
            Config config = new Config();
            config.setCodec(StringCodec.INSTANCE);
            switch (mode) {
                case "cluster":
                    ClusterServersConfig clusterServersConfig = config.useClusterServers();
                    clusterServersConfig.addNodeAddress(nodes.split(","));
                    if (StringUtils.isNotBlank(password)) {
                        clusterServersConfig.setPassword(password);
                    }
                    break;
                default:
                    MasterSlaveServersConfig masterSlaveServersConfig = config.useMasterSlaveServers();
                    masterSlaveServersConfig.setMasterAddress(nodes);
                    if (StringUtils.isNotBlank(password)) {
                        masterSlaveServersConfig.setPassword(password);
                    }
                    break;
            }
            return config;
        }


        private Map<String, String> parseInput(String input) {
            Map<String, String> map = new LinkedHashMap<>();
            String[] kvs = StringUtils.split(input, sourceSchema.getSeparator1());
            if (kvs != null && kvs.length > 0) {
                for (String keyValue : kvs) {
                    if (StringUtils.isBlank(keyValue)) {
                        continue;
                    }
                    int index = keyValue.indexOf("=");
                    if (index > 0) {
                        String key = StringUtils.substring(keyValue, 0, index);
                        if (index == (keyValue.length() - 1)) {
                            map.put(key, "");
                        } else {
                            map.put(key, StringUtils.substring(keyValue, index + 1));
                        }
                    }
                }
            }

            return map;
        }
    }

    public static class SourceSchema {

        private String type;
        private String separator1;
        private String separator2;
        private String charset;

        public static SourceSchema parseSourceSchema(Map<String, String> configMap) throws IOException {
            String schemaString = configMap
                    .getOrDefault("source.schema", "{\"type\":\"kv\",\"separator1\":\"|\",\"separator2\":\"=\"}");
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            schemaString = StringEscapeUtils.unescapeJson(schemaString);
            SourceSchema sourceSchema = objectMapper.readValue(schemaString, SourceSchema.class);
            if (sourceSchema != null && "kv".equalsIgnoreCase(sourceSchema.getType())) {
                if (StringUtils.isNotBlank(sourceSchema.getSeparator1()) && StringUtils
                        .isNotBlank(sourceSchema.getSeparator2())) {
                    return sourceSchema;
                }
            }
            throw new IllegalArgumentException("parseSourceSchema error! source.schema=" + schemaString);
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getSeparator1() {
            return separator1;
        }

        public void setSeparator1(String separator1) {
            this.separator1 = separator1;
        }

        public String getSeparator2() {
            return separator2;
        }

        public void setSeparator2(String separator2) {
            this.separator2 = separator2;
        }

        public String getCharset() {
            return charset;
        }

        public void setCharset(String charset) {
            this.charset = charset;
        }
    }

    public static class JoinRule {

        private String type;
        private List<String> leftFields;
        private List<String> rightFields;

        public static JoinRule parseJoinRule(Map<String, String> configMap) {
            JoinRule joinRule = new JoinRule();
            joinRule.type = configMap.getOrDefault("joinRule.type", "full_join");
            String leftString = configMap.getOrDefault("joinRule.leftFields", "mykey");
            String rightString = configMap.getOrDefault("joinRule.rightFields", "tp_");
            if (StringUtils.isBlank(leftString) || StringUtils.isBlank(rightString)) {
                throw new IllegalArgumentException("parseJoinRule error!");
            }
            joinRule.leftFields = Arrays.asList(leftString.trim().split(","));
            joinRule.rightFields = Arrays.asList(rightString.trim().split(","));
            return joinRule;
        }

        public String getType() {
            return type;
        }

        public List<String> getLeftFields() {
            return leftFields;
        }

        public void setLeftFields(List<String> leftFields) {
            this.leftFields = leftFields;
        }

        public List<String> getRightFields() {
            return rightFields;
        }

        public void setRightFields(List<String> rightFields) {
            this.rightFields = rightFields;
        }
    }

}
