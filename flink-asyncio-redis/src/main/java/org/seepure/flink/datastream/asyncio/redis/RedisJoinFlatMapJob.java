package org.seepure.flink.datastream.asyncio.redis;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.seepure.flink.datastream.asyncio.redis.operator.RedisJoinFlatMap;
import org.seepure.flink.datastream.asyncio.redis.sink.SimpleSinkFunction;
import org.seepure.flink.datastream.asyncio.redis.source.SelfRandomBytesSource;

public class RedisJoinFlatMapJob {

    public static void main(String[] args) throws Exception {
        String defaultRedisHashJoinArg = "{\"cacheType\":\"local\",\"cacheExpireAfterWrite\":20,\"joinRuleLeftFields\":\"user_id\",\"joinRuleRightFields\":\"kh_%s\",\"joinRuleType\":\"inner_join\",\"mode\":\"master-slave\",\"nodes\":\"192.168.234.139:6379\",\"srcTableSchemas\":[{\"contentType\":\"REDIS_HASH\",\"fields\":[{\"fieldIndex\":0,\"fieldKey\":\"gender\",\"fieldName\":\"性别\",\"fieldType\":\"string\"},{\"fieldIndex\":1,\"fieldKey\":\"age\",\"fieldName\":\"年龄\",\"fieldType\":\"string\"},{\"fieldIndex\":2,\"fieldKey\":\"amount\",\"fieldName\":\"总额\",\"fieldType\":\"string\"}],\"formatSpec\":{\"encoding\":\"utf-8\",\"separator\":\"|\"},\"tableName\":\"ods_dim_redis_userprofile\",\"tableType\":\"dim_table\"},{\"contentType\":\"MQ_TEXT\",\"fields\":[{\"fieldIndex\":0,\"fieldKey\":\"country\",\"fieldName\":\"国家\",\"fieldType\":\"string\"},{\"fieldIndex\":1,\"fieldKey\":\"province\",\"fieldName\":\"省份\",\"fieldType\":\"string\"},{\"fieldIndex\":2,\"fieldKey\":\"user_id\",\"fieldName\":\"用户id\",\"fieldType\":\"string\"},{\"fieldIndex\":3,\"fieldKey\":\"event_code\",\"fieldName\":\"事件码\",\"fieldType\":\"string\"},{\"fieldIndex\":4,\"fieldKey\":\"c01\",\"fieldName\":\"自定义参数01\",\"fieldType\":\"string\"},{\"fieldIndex\":5,\"fieldKey\":\"event_time\",\"fieldName\":\"事件发生时间\",\"fieldType\":\"string\"}],\"formatSpec\":{\"encoding\":\"utf-8\",\"separator\":\"|\"},\"tableName\":\"ods_atta_0A2394822347\",\"tableType\":\"stream_table\"}]}";
        String defaultRedisStringJoinArg = "{\"batchSize\":1000,\"cacheExpireAfterWrite\":30,\"cacheNullable\":true,\"cacheSize\":200,\"cacheType\":\"local\",\"joinRuleLeftFields\":\"product_array\",\"joinRuleRightFields\":\"%s\",\"joinRuleType\":\"left_join\",\"minBatchTime\":1000,\"mode\":\"master_slave\",\"nodes\":\"192.168.234.139:6379\",\"srcTableSchemas\":[{\"contentType\":\"MQ_TEXT\",\"fields\":[{\"fieldIndex\":0,\"fieldKey\":\"ftime\",\"fieldName\":\"数据上报时间\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":1,\"fieldKey\":\"extinfo\",\"fieldName\":\"AttaID扩展字段\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":2,\"fieldKey\":\"uin\",\"fieldName\":\"用户唯一标识\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":3,\"fieldKey\":\"order_id\",\"fieldName\":\"用户的订单id\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":4,\"fieldKey\":\"order_action\",\"fieldName\":\"用户操作\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":5,\"fieldKey\":\"order_comment\",\"fieldName\":\"测试订单\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":6,\"fieldKey\":\"order_time_yyyy\",\"fieldName\":\"下单时间_yyyy\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":7,\"fieldKey\":\"order_time_yyyyMM\",\"fieldName\":\"下单时间_yyyyMM\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":8,\"fieldKey\":\"order_time_yyyyMMdd\",\"fieldName\":\"下单时间_yyyyMMdd\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":9,\"fieldKey\":\"order_time_yyyyMMddHH\",\"fieldName\":\"下单时间_yyyyMMddHH\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":10,\"fieldKey\":\"order_time_yyyyMMddHHmm\",\"fieldName\":\"下单时间_yyyyMMddHHmm\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":11,\"fieldKey\":\"order_time_yyyyMMddHHmmss\",\"fieldName\":\"下单时间_yyyyMMddHHmmss\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":12,\"fieldKey\":\"pay_time_yyyy\",\"fieldName\":\"支付时间_yyyy\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":13,\"fieldKey\":\"pay_time_yyyyMM\",\"fieldName\":\"支付时间_yyyyMM\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":14,\"fieldKey\":\"pay_time_yyyyMMdd\",\"fieldName\":\"支付时间_yyyyMMdd\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":15,\"fieldKey\":\"pay_time_yyyyMMddHH\",\"fieldName\":\"支付时间_yyyyMMddHH\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":16,\"fieldKey\":\"pay_time_yyyyMMddHHmm\",\"fieldName\":\"支付时间_yyyyMMddHHmm\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":17,\"fieldKey\":\"pay_time_yyyyMMddHHmmss\",\"fieldName\":\"支付时间_yyyyMMddHHmmss\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":18,\"fieldKey\":\"pay_time_timestamp\",\"fieldName\":\"支付时间_timestamp\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":19,\"fieldKey\":\"pay_time_timestampms\",\"fieldName\":\"支付时间_timestampms\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":20,\"fieldKey\":\"product_name\",\"fieldName\":\"商品名称\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":21,\"fieldKey\":\"product_id\",\"fieldName\":\"商品id\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":22,\"fieldKey\":\"product_sort\",\"fieldName\":\"商品分类\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":23,\"fieldKey\":\"product_collect\",\"fieldName\":\"商品是否被收藏\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":24,\"fieldKey\":\"product_price\",\"fieldName\":\"商品价格\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":25,\"fieldKey\":\"product_url\",\"fieldName\":\"商品url\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":26,\"fieldKey\":\"product_url_encode\",\"fieldName\":\"商品url_encode\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":27,\"fieldKey\":\"product_json\",\"fieldName\":\"商品属性_json\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":28,\"fieldKey\":\"product_kv\",\"fieldName\":\"商品属性_kv\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":29,\"fieldKey\":\"product_array\",\"fieldName\":\"商品数据_array\",\"fieldType\":\"string\",\"id\":null}],\"formatSpec\":{\"encoding\":\"utf-8\",\"separator\":\"|\"},\"tableName\":\"atta_test_dataName_1623574601\"},{\"contentType\":\"MQ_TEXT\",\"tableType\":\"dim_table\",\"fields\":[{\"fieldIndex\":0,\"fieldKey\":\"ftime\",\"fieldName\":\"数据上报时间\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":1,\"fieldKey\":\"extinfo\",\"fieldName\":\"AttaID扩展字段\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":2,\"fieldKey\":\"uin\",\"fieldName\":\"用户唯一标识\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":3,\"fieldKey\":\"order_id\",\"fieldName\":\"用户的订单id\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":4,\"fieldKey\":\"order_action\",\"fieldName\":\"用户操作\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":5,\"fieldKey\":\"order_comment\",\"fieldName\":\"测试订单\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":6,\"fieldKey\":\"order_time_yyyy\",\"fieldName\":\"下单时间_yyyy\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":7,\"fieldKey\":\"order_time_yyyyMM\",\"fieldName\":\"下单时间_yyyyMM\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":8,\"fieldKey\":\"order_time_yyyyMMdd\",\"fieldName\":\"下单时间_yyyyMMdd\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":9,\"fieldKey\":\"order_time_yyyyMMddHH\",\"fieldName\":\"下单时间_yyyyMMddHH\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":10,\"fieldKey\":\"order_time_yyyyMMddHHmm\",\"fieldName\":\"下单时间_yyyyMMddHHmm\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":11,\"fieldKey\":\"order_time_yyyyMMddHHmmss\",\"fieldName\":\"下单时间_yyyyMMddHHmmss\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":12,\"fieldKey\":\"pay_time_yyyy\",\"fieldName\":\"支付时间_yyyy\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":13,\"fieldKey\":\"pay_time_yyyyMM\",\"fieldName\":\"支付时间_yyyyMM\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":14,\"fieldKey\":\"pay_time_yyyyMMdd\",\"fieldName\":\"支付时间_yyyyMMdd\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":15,\"fieldKey\":\"pay_time_yyyyMMddHH\",\"fieldName\":\"支付时间_yyyyMMddHH\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":16,\"fieldKey\":\"pay_time_yyyyMMddHHmm\",\"fieldName\":\"支付时间_yyyyMMddHHmm\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":17,\"fieldKey\":\"pay_time_yyyyMMddHHmmss\",\"fieldName\":\"支付时间_yyyyMMddHHmmss\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":18,\"fieldKey\":\"pay_time_timestamp\",\"fieldName\":\"支付时间_timestamp\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":19,\"fieldKey\":\"pay_time_timestampms\",\"fieldName\":\"支付时间_timestampms\",\"fieldType\":\"double\",\"id\":null},{\"fieldIndex\":20,\"fieldKey\":\"product_name\",\"fieldName\":\"商品名称\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":21,\"fieldKey\":\"product_id\",\"fieldName\":\"商品id\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":22,\"fieldKey\":\"product_sort\",\"fieldName\":\"商品分类\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":23,\"fieldKey\":\"product_collect\",\"fieldName\":\"商品是否被收藏\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":24,\"fieldKey\":\"product_price\",\"fieldName\":\"商品价格\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":25,\"fieldKey\":\"product_url\",\"fieldName\":\"商品url\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":26,\"fieldKey\":\"product_url_encode\",\"fieldName\":\"商品url_encode\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":27,\"fieldKey\":\"product_json\",\"fieldName\":\"商品属性_json\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":28,\"fieldKey\":\"product_kv\",\"fieldName\":\"商品属性_kv\",\"fieldType\":\"string\",\"id\":null},{\"fieldIndex\":29,\"fieldKey\":\"product_array\",\"fieldName\":\"商品数据_array\",\"fieldType\":\"string\",\"id\":null}],\"formatSpec\":{\"encoding\":\"utf-8\",\"separator\":\"|\"},\"tableName\":\"dim_redis_table_name\"}]}";
        //String exceptionJoinArg = "{\"cacheType\":\"local\",\"cacheExpireAfterWrite\":20,\"joinRuleLeftFields\":\"user_id\",\"joinRuleRightFields\":\"ks_%s\",\"joinRuleType\":\"left_join\",\"mode\":\"master-slave\",\"nodes\":\"192.168.234.139:6379\",\"srcTableSchemas\":[{\"contentType\":\"MQ_TEXT\",\"fields\":[{\"fieldIndex\":0,\"fieldKey\":\"country\",\"fieldName\":\"国家\",\"fieldType\":\"string\"},{\"fieldIndex\":1,\"fieldKey\":\"province\",\"fieldName\":\"省份\",\"fieldType\":\"string\"},{\"fieldIndex\":2,\"fieldKey\":\"user_id\",\"fieldName\":\"用户id\",\"fieldType\":\"string\"},{\"fieldIndex\":3,\"fieldKey\":\"event_code\",\"fieldName\":\"事件码\",\"fieldType\":\"string\"},{\"fieldIndex\":4,\"fieldKey\":\"c01\",\"fieldName\":\"自定义参数01\",\"fieldType\":\"string\"},{\"fieldIndex\":5,\"fieldKey\":\"event_time\",\"fieldName\":\"事件发生时间\",\"fieldType\":\"string\"}],\"formatSpec\":{\"encoding\":\"utf-8\",\"separator\":\"|\"},\"tableName\":\"ods_atta_0A2394822347\",\"tableType\":\"stream_table\"}]}";
        String arg = args != null && args.length >= 1 ? args[0] : defaultRedisStringJoinArg;
        StreamExecutionEnvironment env = getEnv(null);
        DataStream<byte[]> in = env.addSource(new SelfRandomBytesSource("mykey", 10, 1000));
        SingleOutputStreamOperator<String> stream = in.flatMap(new RedisJoinFlatMap(arg));
        stream.addSink(new SimpleSinkFunction());
        env.execute();
    }

    private static StreamExecutionEnvironment getEnv(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (params != null)
            env.getConfig().setGlobalJobParameters(params);
        env.getConfig().enableObjectReuse();

        int parallel = Integer.parseInt(params == null ? "1" : params.get("parallel", "1"));
        env.setParallelism(parallel);
        return env;
    }

}
