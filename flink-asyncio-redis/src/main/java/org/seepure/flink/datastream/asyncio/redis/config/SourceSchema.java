package org.seepure.flink.datastream.asyncio.redis.config;

import java.util.Map;

public abstract class SourceSchema /*implements DeserializationSchema<Map<String, String>>*/ {

    public static SourceSchema getSourceSchema(Map<String, String> configMap) throws Exception {
        String type = configMap.getOrDefault("source.schema.type", "kv_text");
        String schemaContent = configMap.get("source.schema.content");
        SourceSchema sourceSchema = null;
        switch (type) {
            case "kv_text" :
                sourceSchema = new KvTextSourceSchema();
                sourceSchema.parseConfig(schemaContent);
                break;
            default:
                throw new IllegalArgumentException("unsupported source.schema.type: " + type);
        }
        return sourceSchema;
    }

    public abstract void parseConfig(String configContent) throws Exception;

    public abstract Map<String, String> parseInput(byte[] bytes);

    public abstract Map<String, String> parseInput(String input);

}
