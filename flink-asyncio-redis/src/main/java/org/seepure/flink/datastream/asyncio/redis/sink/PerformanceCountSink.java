package org.seepure.flink.datastream.asyncio.redis.sink;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceCountSink extends RichSinkFunction<String> {
    private static Logger LOG = LoggerFactory.getLogger(PerformanceCountSink.class);
    private AtomicLong acl;
    private long lastTime;
    private Thread thread;
    private volatile boolean running = true;
    private volatile String msg;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        acl = new AtomicLong(0);
        lastTime = System.currentTimeMillis();
        thread = new Thread(() -> {
            while (running) {
                long now = System.currentTimeMillis();
                if (now - lastTime >= 10_000) {
                    LOG.info("processed " + acl.get() + " msg(s) in last 10s.");
                    lastTime = now;
                    acl.set(0);
                    LOG.info("msg: " + msg);
                    msg = null;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {

                }
            }
        });
        thread.start();
    }

    @Override
    public void close() throws Exception {
        running = false;
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        acl.incrementAndGet();
        if (msg == null) {
            msg = value;
        }
    }
}
