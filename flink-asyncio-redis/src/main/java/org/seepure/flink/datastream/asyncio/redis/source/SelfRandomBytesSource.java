package org.seepure.flink.datastream.asyncio.redis.source;

import java.util.Date;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelfRandomBytesSource extends RichSourceFunction<byte[]> {

    private static Logger LOG = LoggerFactory.getLogger(SelfRandomBytesSource.class);

    private volatile boolean running = true;
    private String keyColumn;
    private int bound;
    private long interval;

    public SelfRandomBytesSource(String keyColumn, int bound, long interval) {
        this.keyColumn = keyColumn;
        this.bound = bound;
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) throws Exception {
        final Random random = new Random();
        int num = 0;
        while (running) {
            num = num % bound;
            //String msg = num + "|" + random.nextInt(100) + "|" + "CN";
            //String msg = "CN|GD|" + num  + "|event_code|c01|"+ new Date().toString();
            String msg = "2021-06-13 18:29:25.348|a=100.107.137.196&extinfo=100.107.137.196|93828607133aa|14669797369|pay|http%3A%2F%2Ftool.chinaz.com%2Ftools%2Furlencode.aspx|2021|2021-06|2021-06-13|2021-06-13 18|2021-06-13 18:29|2021-06-13 18:29:15|2021|202106|20210613|2021061318|202106131829|20210613182925|||testa|001|A|0|123.123|http://test.datahubweb.wsd.com/config?type=testa|http://test.datahubweb.wsd.com/config?type=testa&product_id=001|{\"param\":{\"platform\":\"Android\",\"sub_tab_info\":[{\"product_id\":\"001\",\"product_name\":\"testa\",\"product_price\":123.123}]}}|product_id_kv=001#product_name_kv=testa#product_price_kv=123.123|001#testa#A";
            ctx.collect(msg.getBytes());
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
