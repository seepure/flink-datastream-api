package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

@Deprecated
public abstract class SourceSchema implements Serializable /*implements DeserializationSchema<Map<String, String>>*/ {

    public static SourceSchema getSourceSchema(Map<String, String> configMap) throws Exception {
        String type = configMap.get("source.schema.type");
        if (StringUtils.isBlank(type)) {
            type = configMap.get("sourceSchemaType");
        }
        String schemaContent = configMap.get("source.schema.content");
        if (StringUtils.isBlank(schemaContent)) {
            schemaContent = configMap.get("sourceSchemaContent");
        }
        SourceSchema sourceSchema = null;
        switch (type) {
            case "MQ_KV":
                sourceSchema = new KvTextSourceSchema();
                break;
            case "MQ_TEXT":
                sourceSchema = new MQTextSourceSchema();
                break;
            default:
                throw new IllegalArgumentException("unsupported source.schema.type: " + type);
        }
        sourceSchema.parseConfig(schemaContent);
        return sourceSchema;
    }

    public abstract void parseConfig(String configContent) throws Exception;

    public abstract Map<String, String> parseInput(byte[] bytes);

    public abstract Map<String, String> parseInput(String input);

}
