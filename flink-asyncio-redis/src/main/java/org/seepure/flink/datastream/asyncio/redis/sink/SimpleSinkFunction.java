package org.seepure.flink.datastream.asyncio.redis.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSinkFunction extends RichSinkFunction<String> {

    private static Logger LOG = LoggerFactory.getLogger(SimpleSinkFunction.class);

    @Override
    public void invoke(String value, Context context) throws Exception {
        LOG.info("sink: " + value);
    }
}
