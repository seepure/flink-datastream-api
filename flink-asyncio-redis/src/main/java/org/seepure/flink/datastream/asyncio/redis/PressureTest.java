package org.seepure.flink.datastream.asyncio.redis;

import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.seepure.flink.datastream.asyncio.redis.OptimizedBatchIOJob.OptimizedRedisBatchFlatMap;
import org.seepure.flink.datastream.asyncio.redis.sink.PerformanceCountSink;
import org.seepure.flink.datastream.asyncio.redis.source.PressureRandomSource;
import org.seepure.flink.datastream.asyncio.redis.util.ArgUtil;

public class PressureTest {

    public static void main(String[] args) throws Exception {
        String defaultRedisStringJoinArg =
                "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                        + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.kv_text;dim.schema.content={};joinRule.rightFields=tp_%s";
        String defaultRedisHashJoinArg =
                "redis.mode=cluster;redis.nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001"
                        + ";source.schema.type=MQ_KV;source.schema.content={};dim.schema.type=redis.hash;dim.schema.content={};joinRule.rightFields=th_%s";
        String arg = args != null && args.length >= 1 ? args[0] : defaultRedisHashJoinArg;
        //"redis.mode=cluster;redis.nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001";
        Map<String, String> configMap = ArgUtil.getArgMapFromArgs(arg);
        configMap.put("redis.nodes", "redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001");
        //configMap.put("redis.nodes", "redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001");
        ParameterTool params = ParameterTool.fromMap(configMap);
        StreamExecutionEnvironment env = getEnv(params);
        DataStream<String> in = env.addSource(new PressureRandomSource("mykey", 50000, 1000));
        SingleOutputStreamOperator<String> stream = in.flatMap(new OptimizedRedisBatchFlatMap(configMap));
        stream.addSink(new PerformanceCountSink());
        env.execute();
    }
    private static StreamExecutionEnvironment getEnv(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
        return env;
    }
}
