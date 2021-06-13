package org.seepure.flink.datastream.asyncio.redis.config;

import java.io.Serializable;
import java.util.Map;
import org.seepure.flink.datastream.asyncio.redis.config.RedisJoinConfig.TableSchema;

public abstract class TableSchemaExecutor<IN> implements Serializable {

    public abstract void parseConfig(TableSchema tableSchema);

    public abstract Map<String, String> parseInput(IN input);

    public abstract Map<String, String> parseBytes(byte[] bytes);

}
