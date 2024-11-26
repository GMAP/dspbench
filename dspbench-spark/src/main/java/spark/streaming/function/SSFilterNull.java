package spark.streaming.function;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 *
 * @author luandopke
 */
public class SSFilterNull<K, V> extends BaseFunction implements FilterFunction<Row> {

    @Override
    public boolean call(Row input) throws Exception {
        return input != null;
    }
}
