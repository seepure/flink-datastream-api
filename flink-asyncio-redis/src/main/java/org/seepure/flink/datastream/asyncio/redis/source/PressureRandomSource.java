package org.seepure.flink.datastream.asyncio.redis.source;

import java.util.Random;
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
        Random random = new Random();
        while (running) {
            int num = -1;
            int classNum = random.nextInt(100);
            if (classNum < 25) {
                num = random.nextInt(bound / 100);
            } else if (classNum < 40) {
                num = random.nextInt(bound * 5 / 100);
            } else if (classNum < 65) {
                num = random.nextInt(bound / 10);
            } else if (classNum < 90) {
                num = bound / 10 + random.nextInt(bound * 2 / 10);
            } else {
                num = bound * (1 + 2) / 10 + random.nextInt(bound * 7 / 10);
            }
            String msg = keyColumn + "=" + num;
            ctx.collect(msg);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
