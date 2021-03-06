package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class DimRedisHashSchema extends DimRedisSchema<Map<String, String>> {

    @Override
    public void parseConfig(String configContent) throws IOException {

    }

    @Override
    public Map<String, String> parseInput(Map<String, String> input) {
        if (input == null) {
            return new LinkedHashMap<>();
        }
        return input;
    }
}
