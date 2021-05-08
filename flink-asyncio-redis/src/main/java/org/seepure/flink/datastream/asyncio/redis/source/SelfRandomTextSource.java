package org.seepure.flink.datastream.asyncio.redis.source;

import java.util.Random;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelfRandomTextSource extends RichSourceFunction<String> {

    private static Logger LOG = LoggerFactory.getLogger(SelfRandomTextSource.class);

    private volatile boolean running = true;
    private String keyColumn;
    private int bound;
    private long interval;

    public SelfRandomTextSource(String keyColumn, int bound, long interval) {
        this.keyColumn = keyColumn;
        this.bound = bound;
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        final Random random = new Random();
        int num = 0;
        while (running) {
            num = num % bound;
            String msg = num + "|" + random.nextInt(100) + "|" + "CN";
            ctx.collect(msg);
            //System.out.println("source=" + msg);
            LOG.info("source=" + msg);
            Thread.sleep(interval);
            ++num;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
