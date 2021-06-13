package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig.TableSchema;
import org.seepure.flink.datastream.asyncio.redis.util.AssertUtil;

public class KvTextSchemaExecutor extends TableSchemaExecutor<String> {

    private String separator;
    private String kvSeparator;
    private String charset;

    @Override
    public void parseConfig(TableSchema tableSchema) {
        AssertUtil.assertTrue(tableSchema != null, "tableSchema is empty!");
        Map<String, Object> formatSpec = tableSchema.getFormatSpec();
        if (formatSpec == null) {
            formatSpec = Collections.emptyMap();
        }
        separator = (String) formatSpec.getOrDefault("separator", "|");
        kvSeparator = (String) formatSpec.getOrDefault("kvSeparator", "=");
        charset = (String) formatSpec.getOrDefault("encoding", "UTF-8");
        AssertUtil.assertTrue(StringUtils.isNotBlank(separator) && StringUtils.isNotBlank(kvSeparator) && StringUtils
                        .isNotBlank(charset),
                "Illegal config for KvTextSchemaExecutor");
    }

    @Override
    public Map<String, String> parseBytes(byte[] bytes) {
        try {
            String content = new String(bytes, charset);
            return parseInput(content);
        } catch (UnsupportedEncodingException e) {
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, String> parseInput(String input) {
        Map<String, String> map = new LinkedHashMap<>();
        String[] kvs = StringUtils.split(input, separator);
        if (kvs != null && kvs.length > 0) {
            for (String keyValue : kvs) {
                if (StringUtils.isBlank(keyValue)) {
                    continue;
                }
                int index = keyValue.indexOf(kvSeparator);
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
