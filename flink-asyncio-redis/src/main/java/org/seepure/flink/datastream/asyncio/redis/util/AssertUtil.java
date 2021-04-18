package org.seepure.flink.datastream.asyncio.redis.util;

public class AssertUtil {
    public static void assertTrue(boolean expression, String msg) {
        if (!expression) {
            throw new IllegalArgumentException(msg);
        }
    }
}
