package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;

@Deprecated
public class DimRedisSplitTextSchema extends DimRedisSchema<String> {

    private String[] indexToFieldKey;

    @Override
    public void parseConfig(String configContent) throws IOException {
        indexToFieldKey = MQTextSourceSchema.split(configContent, "|");
        AssertUtil.assertTrue(indexToFieldKey != null && indexToFieldKey.length >= 1,
                "parseConfig error! indexToFieldKey is empty");
    }

    @Override
    public Map<String, String> parseInput(String input) {
        Map<String, String> map = new LinkedHashMap<>();
        if (StringUtils.isBlank(input)) {
            return map;
        }
        String[] kvs = StringUtils.split(input, "|");
        if (kvs != null && kvs.length > 0) {
            for (int i = 0; i < kvs.length && i < indexToFieldKey.length; i++) {
                map.put(indexToFieldKey[i], kvs[i]);
            }
        }
        return map;
    }


}
