package org.seepure.flink.datastream.asyncio.redis.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class ArgUtil {
    private static Logger LOG = LoggerFactory.getLogger(ArgUtil.class);

    public static Map<String, String> getArgMapFromArgs(String arg) {
        Map<String, String> record = new LinkedHashMap<>();
        if (StringUtils.isNotBlank(arg)) {
            LOG.debug("arg: " + arg);
            String[] kvs = StringUtils.split(arg, ';');
            LOG.debug("kvs.length = " + kvs.length);
            for (String kv : kvs) {
                String[] kvPair = StringUtils.split(kv, '=');
                record.put(kvPair[0], kvPair[1]);
            }
        }
        return record;
    }
}
