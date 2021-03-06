package org.seepure.flink.datastream.asyncio.redis.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;
import org.seepure.flink.datastream.asyncio.redis.util.JsonUtil;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class DimRedisKvTextSchema extends DimRedisSchema<String> {
    private String separator1;
    private String separator2;

    @Override
    public void parseConfig(String configContent) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        configContent = StringEscapeUtils.unescapeJson(configContent);
        JsonNode jsonNode = objectMapper.readTree(configContent);
        separator1 = JsonUtil.getStringOrDefault(jsonNode, "separator1", "|");
        separator2 = JsonUtil.getStringOrDefault(jsonNode, "separator2", "=");
        AssertUtil.assertTrue(StringUtils.isNotBlank(separator1) && StringUtils.isNotBlank(separator2),
                "Illegal config for KvTextSourceSchema");
    }

    @Override
    public Map<String, String> parseInput(String input) {
        Map<String, String> map = new LinkedHashMap<>();
        if (StringUtils.isBlank(input)) {
            return map;
        }
        String[] kvs = StringUtils.split(input, separator1);
        if (kvs != null && kvs.length > 0) {
            for (String keyValue : kvs) {
                if (StringUtils.isBlank(keyValue)) {
                    continue;
                }
                int index = keyValue.indexOf(separator2);
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
