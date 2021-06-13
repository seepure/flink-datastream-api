package org.seepure.flink.datastream.asyncio.redis.config;


import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig.TableSchema;

public class TableSchemaExecutorBuilder {

    public static final String MQ_TEXT = "MQ_TEXT";
    public static final String MQ_KV = "MQ_KV";
    public static final String REDIS_HASH = "REDIS_HASH";

    public static TableSchemaExecutor build(TableSchema tableSchema) {
        if (tableSchema == null || tableSchema.getContentType() == null) {
            throw new IllegalArgumentException("empty tableSchema or empty tableSchema.getContentType()! ");
        }
        TableSchemaExecutor schemaExecutor = null;
        switch (tableSchema.getContentType()) {
            case MQ_TEXT:
                schemaExecutor = new CsvSchemaExecutor();
                break;
            case MQ_KV:
                schemaExecutor = new KvTextSchemaExecutor();
                break;
            case REDIS_HASH:
                schemaExecutor = new RedisHashSchemaExecutor();
                break;
            default:
                throw new IllegalArgumentException(
                        "unsupported tableSchema contentType : " + tableSchema.getContentType());
        }
        schemaExecutor.parseConfig(tableSchema);
        return schemaExecutor;
    }

}
