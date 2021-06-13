package org.seepure.flink.datastream.asyncio.redis.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig.TableSchema;

public class RedisHashSchemaExecutor extends TableSchemaExecutor<Map<String, String>> {

    @Override
    public void parseConfig(TableSchema tableSchema) {

    }

    @Override
    public Map<String, String> parseInput(Map<String, String> input) {
        if (input == null) {
            return new LinkedHashMap<>();
        }
        return input;
    }

    @Override
    public Map<String, String> parseBytes(byte[] bytes) {
        return null;
    }
}
