package org.seepure.flink.datastream.asyncio.redis.config;

import org.apache.commons.lang3.StringUtils;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;

import java.util.Map;

public class RedissonConfig {
    public static Config getRedissonConfig(Map<String, String> configMap) {
        String mode = configMap.get("redis.mode");
        if (StringUtils.isBlank(mode)) {
            mode = configMap.getOrDefault("mode", "").toLowerCase();
        }
        String nodes = configMap.get("redis.nodes");
        if (StringUtils.isBlank(nodes)) {
            nodes = configMap.get("nodes");
        }
        String password = configMap.get("redis.auth");
        if (password == null) {
            password = configMap.get("auth");
        }
        Config config = new Config();
        config.setCodec(StringCodec.INSTANCE);
        switch (mode) {
            case "cluster":
                ClusterServersConfig clusterServersConfig = config.useClusterServers();
                String[] addresses = nodes.split(",");
                if (addresses == null || addresses.length < 1) {
                    throw new IllegalArgumentException("empty config nodes for redis.");
                }
                for (int i=0; i < addresses.length; i++) {
                    if (!addresses[i].startsWith("redis://")) {
                        addresses[i] = "redis://" + addresses[i];
                    }
                }
                clusterServersConfig.addNodeAddress(addresses);
                if (StringUtils.isNotBlank(password)) {
                    clusterServersConfig.setPassword(password);
                }
                break;
            default:
                MasterSlaveServersConfig masterSlaveServersConfig = config.useMasterSlaveServers();
                if (StringUtils.isBlank(nodes)) {
                    throw new IllegalArgumentException("empty config nodes for redis.");
                }
                if (!nodes.startsWith("redis://")) {
                    nodes = "redis://" + nodes;
                }
                masterSlaveServersConfig.setMasterAddress(nodes);
                if (StringUtils.isNotBlank(password)) {
                    masterSlaveServersConfig.setPassword(password);
                }
                break;
        }
        return config;
    }
}
