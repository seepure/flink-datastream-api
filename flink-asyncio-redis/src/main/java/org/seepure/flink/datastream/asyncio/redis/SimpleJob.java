package org.seepure.flink.datastream.asyncio.redis;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

public class SimpleJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = getEnv(params);
        String host = params.get("socket_host", "192.168.213.1");
        int port = Integer.parseInt(params.get("socket_port", "8888"));
        DataStreamSource<String> stream = env.socketTextStream(host, port);
        stream.flatMap(new SimpleRedisJoin()).print();
        env.execute();
    }

    private static StreamExecutionEnvironment getEnv(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
        return env;
    }

    public static class SimpleRedisJoin extends RichFlatMapFunction<String, String> {

        private transient RedissonClient client;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            String mode = globalParams.toMap().getOrDefault("redis.mode", "masterSlaves");
            String redisMaster = globalParams.toMap().getOrDefault("redis.master", "redis://192.168.213.128:6379");
            String redisSlave = globalParams.toMap().getOrDefault("redis.slave", "redis://192.168.213.129:6379");
            Config config = new Config();
            config.setCodec(StringCodec.INSTANCE);
            config.useMasterSlaveServers().setMasterAddress(redisMaster).addSlaveAddress(redisSlave);
            client = Redisson.create(config);
        }

        @Override
        public void close() throws Exception {
            client.shutdown();
            super.close();
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (StringUtils.isBlank(value)) {
                return;
            }
            RBucket<Object> bucket = client.getBucket(value);
            Object o = bucket.get();
            if (o != null) {
                out.collect(String.format("key=%s&value=%s", value, o.toString()));
            }
        }
    }

}
