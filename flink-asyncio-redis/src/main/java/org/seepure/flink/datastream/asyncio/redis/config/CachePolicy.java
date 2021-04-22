package org.seepure.flink.datastream.asyncio.redis.config;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

public class CachePolicy {

    public final static Object EMPTY_CACHE_RESULT = new Object();
    private String type;
    private int size;
    private boolean loadOnBeginning = false;
    //todo 支持nullable的缓存策略
    private boolean nullable = false;
    private long expireAfterWrite;

    public CachePolicy() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isLoadOnBeginning() {
        return loadOnBeginning;
    }

    public void setLoadOnBeginning(boolean loadOnBeginning) {
        this.loadOnBeginning = loadOnBeginning;
    }

    public long getExpireAfterWrite() {
        return expireAfterWrite;
    }

    public void setExpireAfterWrite(long expireAfterWrite) {
        this.expireAfterWrite = expireAfterWrite;
    }

    public enum DimUpdatePolicy {
        MINUTE(15, 15),
        HOUR(300, 300),
        DAY(1800, 1800),
        RANDOM(-1, -1)
        ;

        public final int expireDuration;
        public final int refreshDuration;

        DimUpdatePolicy(int expireDuration, int refreshDuration) {
            this.expireDuration = expireDuration;
            this.refreshDuration = refreshDuration;
        }

        public static DimUpdatePolicy matches(String name) {
            return Stream.of(DimUpdatePolicy.values())
                    .filter(e -> Objects.equals(e.name(), name)).findAny().orElse(null);
        }
    }

    public static CachePolicy getCachePolicy(Map<String, String> configMap) {
        CachePolicy cachePolicy = new CachePolicy();
        String type = configMap.get("cachePolicy.type");
        if (!Objects.equals(type, "local")) {
            return null;
        }
        cachePolicy.setType(type);
        String expireAfterWrite = configMap.get("cachePolicy.expireAfterWrite");
        if (StringUtils.isNotBlank(expireAfterWrite)) {
            long ttl = Long.parseLong(expireAfterWrite);
            cachePolicy.setExpireAfterWrite(ttl);
        } else {
            String dimUpdateType = configMap.get("cachePolicy.dimUpdatePolicy");
            CachePolicy.DimUpdatePolicy dimUpdatePolicy = DimUpdatePolicy.matches(dimUpdateType.toUpperCase());
            if (dimUpdatePolicy == null || dimUpdatePolicy == DimUpdatePolicy.RANDOM) {
                return null;
            }
            cachePolicy.setExpireAfterWrite(dimUpdatePolicy.expireDuration);
        }

        String loadOnBeginning = configMap.getOrDefault("cachePolicy.loadOnBeginning", "false");
        String size = configMap.get("cachePolicy.size");

        cachePolicy.setSize(Integer.parseInt(size));
        cachePolicy.setLoadOnBeginning(Boolean.getBoolean(loadOnBeginning));
        return cachePolicy;
    }

}
