package spark.streaming.sink;

import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import spark.streaming.function.BaseFunction;
import spark.streaming.metrics.Latency;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class PairSink<K> extends BaseFunction implements VoidFunction<JavaPairRDD<K, Tuple>> {

    public PairSink(Configuration config) {
        super(config);
    }

    public PairSink(Configuration config, String name) {
        super(config, name);
    }

    @Override
    public void call(JavaPairRDD<K, Tuple> rdd) throws Exception {
        //incReceived(rdd.count());
    }
}
