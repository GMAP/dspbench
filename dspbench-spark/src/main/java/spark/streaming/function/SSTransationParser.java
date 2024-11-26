package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.util.Configuration;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSTransationParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSTransationParser.class);
    private static Map<String, Long> throughput= new HashMap<>();;
    private static BlockingQueue<String> queue;
    public SSTransationParser(Configuration config) {
        super(config);
        queue = new ArrayBlockingQueue<>(20);
    }

    @Override
    public Row call(String value) throws Exception {
        incReceived();
        //Calculate();
        try {
            String[] items = value.split(",", 2);
            incEmitted();
            return RowFactory.create(items[0], items[1], Instant.now().toEpochMilli());
        } catch (NumberFormatException ex) {
            LOG.error("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.error("Error parsing date/time value", ex);
        }

        return null;
    }

    public void Calculate() throws InterruptedException {
        //Tuple2<Map<String, Long>, BlockingQueue<String>> d = super.calculateThroughput(throughput, queue);
        //throughput = d._1;
        //queue = d._2;
        //if (queue.size() >= 10) {
        //    super.SaveMetrics(queue.take());
        //}
    }
}