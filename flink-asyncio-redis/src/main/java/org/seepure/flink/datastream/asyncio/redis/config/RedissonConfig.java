package org.seepure.flink.datastream.asyncio.redis.config;

import org.apache.commons.lang3.StringUtils;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;

import java.util.Map;

public class RedissonConfig {
    public static Config getRedissonConfig(Map<String, String> configMap) {
        String mode = configMap.getOrDefault("redis.mode", "").toLowerCase();
        String nodes = configMap.getOrDefault("redis.nodes", "redis://192.168.234.137:6379");
        String password = configMap.get("redis.auth");
        Config config = new Config();
        config.setCodec(StringCodec.INSTANCE);
        switch (mode) {
            case "cluster":
                ClusterServersConfig clusterServersConfig = config.useClusterServers();
                clusterServersConfig.addNodeAddress(nodes.split(","));
                if (StringUtils.isNotBlank(password)) {
                    clusterServersConfig.setPassword(password);
                }
                break;
            default:
                MasterSlaveServersConfig masterSlaveServersConfig = config.useMasterSlaveServers();
                masterSlaveServersConfig.setMasterAddress(nodes);
                if (StringUtils.isNotBlank(password)) {
                    masterSlaveServersConfig.setPassword(password);
                }
                break;
        }
        return config;
    }
}
