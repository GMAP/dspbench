package spark.streaming.sink;

import com.codahale.metrics.Timer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import spark.streaming.function.BaseFunction;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author mayconbordin
 */
public class Sink<K> extends BaseFunction implements Function<JavaRDD<Tuple2<K, Tuple>>, Void> {

    public Sink(Configuration config) {
        super(config);
    }

    public Sink(Configuration config, String name) {
        super(config, name);
    }

    @Override
    public Void call(JavaRDD<Tuple2<K, Tuple>> rdd) throws Exception {
        //incReceived(rdd.count());
        
        return null;
    }
    
}
