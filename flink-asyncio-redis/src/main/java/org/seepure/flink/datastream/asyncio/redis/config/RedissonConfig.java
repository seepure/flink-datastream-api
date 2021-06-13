package org.seepure.flink.datastream.asyncio.redis.config;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;

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
                for (int i = 0; i < addresses.length; i++) {
                    if (!addresses[i].startsWith("redis://")) {
                        addresses[i] = "redis://" + addresses[i];
                    }
                }
                clusterServersConfig.addNodeAddress(addresses);
                if (StringUtils.isNotBlank(password)) {
                    clusterServersConfig.setPassword(getAuth(password));
                }
                clusterServersConfig.setMasterConnectionMinimumIdleSize(2);
                clusterServersConfig.setMasterConnectionPoolSize(16);
                clusterServersConfig.setSlaveConnectionMinimumIdleSize(2);
                clusterServersConfig.setSlaveConnectionPoolSize(16);
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
                    masterSlaveServersConfig.setPassword(getAuth(password));
                }
                masterSlaveServersConfig.setMasterConnectionMinimumIdleSize(2);
                masterSlaveServersConfig.setMasterConnectionPoolSize(16);
                masterSlaveServersConfig.setSlaveConnectionMinimumIdleSize(2);
                masterSlaveServersConfig.setSlaveConnectionPoolSize(16);
                break;
        }
        return config;
    }

    public static Config getRedissonConfig(RedisJoinConfig redisJoinConfig) {
        if (redisJoinConfig == null) {
            return null;
        }
        Config config = new Config();
        config.setCodec(StringCodec.INSTANCE);
        String mode = redisJoinConfig.getMode() != null ? redisJoinConfig.getMode() : "";
        String nodes = redisJoinConfig.getNodes();
        if (StringUtils.isBlank(nodes)) {
            throw new IllegalArgumentException("empty config nodes for redis.");
        }
        switch (mode) {
            case "cluster":
                ClusterServersConfig clusterServersConfig = config.useClusterServers();
                String[] addresses = nodes.split(",");
                if (addresses == null || addresses.length < 1) {
                    throw new IllegalArgumentException("empty config nodes for redis.");
                }
                for (int i = 0; i < addresses.length; i++) {
                    if (!addresses[i].startsWith("redis://")) {
                        addresses[i] = "redis://" + addresses[i];
                    }
                }
                clusterServersConfig.addNodeAddress(addresses);
                if (StringUtils.isNotBlank(redisJoinConfig.getAuth())) {
                    clusterServersConfig.setPassword(getAuth(redisJoinConfig.getAuth()));
                }
                clusterServersConfig.setMasterConnectionMinimumIdleSize(2);
                clusterServersConfig.setMasterConnectionPoolSize(16);
                clusterServersConfig.setSlaveConnectionMinimumIdleSize(2);
                clusterServersConfig.setSlaveConnectionPoolSize(16);
                break;
            default:
                MasterSlaveServersConfig masterSlaveServersConfig = config.useMasterSlaveServers();
                if (!nodes.startsWith("redis://")) {
                    nodes = "redis://" + nodes;
                }
                masterSlaveServersConfig.setMasterAddress(nodes);
                if (StringUtils.isNotBlank(redisJoinConfig.getAuth())) {
                    masterSlaveServersConfig.setPassword(getAuth(redisJoinConfig.getAuth()));
                }
                masterSlaveServersConfig.setMasterConnectionMinimumIdleSize(2);
                masterSlaveServersConfig.setMasterConnectionPoolSize(16);
                masterSlaveServersConfig.setSlaveConnectionMinimumIdleSize(2);
                masterSlaveServersConfig.setSlaveConnectionPoolSize(16);
                break;
        }
        return config;
    }

    private static String getAuth(String auth) {
//        if (StringUtils.isNotBlank(auth)) {
//            Properties properties = AesUtil.getConfig(AesUtil.CONFIG_NAME);
//            if (properties != null) {
//                return AesUtil.decode(properties.getProperty(AesUtil.APP_SECRET_KEY), auth);
//            }
//        }
        return auth;
    }
}
