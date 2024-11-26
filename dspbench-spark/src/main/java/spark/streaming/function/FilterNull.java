package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 *
 * @author mayconbordin
 */
public class FilterNull<K, V> extends BaseFunction implements Function<Tuple2<K, V>, Boolean> {

    @Override
    public Boolean call(Tuple2<K, V> input) throws Exception {
        return input != null;
    }
}
