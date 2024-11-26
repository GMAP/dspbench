package spark.streaming.function;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import scala.Tuple2;
import spark.streaming.util.Configuration;

import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSWordCount extends BaseFunction implements MapGroupsWithStateFunction<String, Row, Long, Row> {

    public SSWordCount(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String key, Iterator<Row> values, GroupState<Long> state) throws Exception {
        long count = 0;
        while (values.hasNext()) {
            values.next();

            if (state.exists()) {
                count = state.get();
            }
            count++;
            state.update(count);

            incBoth();
            //recemitThroughput();
        }
        return RowFactory.create(key, count);
    }
}