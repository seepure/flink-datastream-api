package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.IOException;
import java.util.Map;

public abstract class DimSchema<IN> /*implements DeserializationSchema<Map<String, String>>*/ {

    public static DimSchema getDimSchema(Map<String, String> configMap) throws IOException {
        String type = configMap.get("dim.schema.type");
        String schemaContent = configMap.get("dim.schema.content");
        DimSchema dimSchema = null;
        switch (type) {
            case "redis.kv_text":
                dimSchema = new DimRedisKvTextSchema();
                dimSchema.parseConfig(schemaContent);
                break;
            case "redis.hash":
                dimSchema = new DimRedisHashSchema();
                dimSchema.parseConfig(schemaContent);
                break;
            default:
                throw new IllegalArgumentException("unsupported dim.schema.type: " + type);
        }
        return dimSchema;
    }

    public abstract void parseConfig(String configContent) throws IOException;

    public abstract Map<String, String> parseInput(IN input);

}
