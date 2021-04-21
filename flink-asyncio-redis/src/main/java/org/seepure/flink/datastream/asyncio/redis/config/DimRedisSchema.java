package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public abstract class DimRedisSchema<IN> implements Serializable /*implements DeserializationSchema<Map<String, String>>*/ {

    public static DimRedisSchema getDimSchema(Map<String, String> configMap) throws IOException {
        String type = configMap.get("dim.schema.type");
        if (StringUtils.isBlank(type)) {
            type = configMap.get("dataSchema");
        }
        String schemaContent = configMap.get("dim.schema.content");
        if (StringUtils.isBlank(schemaContent)) {
            schemaContent = configMap.getOrDefault("dataSchemaDesc", "{}");
        }
        DimRedisSchema dimRedisSchema = null;
        switch (type) {
            case "string":
            case "redis.kv_text":
                dimRedisSchema = new DimRedisKvTextSchema();
                dimRedisSchema.parseConfig(schemaContent);
                break;
            case "hash":
            case "redis.hash":
                dimRedisSchema = new DimRedisHashSchema();
                dimRedisSchema.parseConfig(schemaContent);
                break;
            default:
                throw new IllegalArgumentException("unsupported dim.schema.type/dataSchema: " + type);
        }
        return dimRedisSchema;
    }

    public abstract void parseConfig(String configContent) throws IOException;

    public abstract Map<String, String> parseInput(IN input);

}
