package org.seepure.flink.datastream.asyncio.redis.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PressureRandomSource extends RichSourceFunction<String> {

    private static Logger LOG = LoggerFactory.getLogger(SelfRandomSource.class);

    private volatile boolean running = true;
    private String keyColumn;
    private int bound;
    private long interval;

    public PressureRandomSource(String keyColumn, int bound, long interval) {
        this.keyColumn = keyColumn;
        this.bound = bound;
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
