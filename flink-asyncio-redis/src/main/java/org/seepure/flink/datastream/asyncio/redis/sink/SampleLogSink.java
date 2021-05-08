package org.seepure.flink.datastream.asyncio.redis.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleLogSink extends RichSinkFunction<String> {

    private static Logger LOG = LoggerFactory.getLogger(SimpleSinkFunction.class);
    private volatile long lastTime = 0;
    private final long interval;

    public SampleLogSink(long interval) {
        this.interval = interval;
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastTime >= interval) {
            lastTime = now;
            LOG.info("sink: " + value);
        }
    }
}
