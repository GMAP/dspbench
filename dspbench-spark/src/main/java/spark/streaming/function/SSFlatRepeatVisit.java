package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import scala.Tuple2;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSFlatRepeatVisit extends BaseFunction implements FlatMapGroupsWithStateFunction<String, Row, Boolean, Row> {
 //   private static Map<String, Long> throughput = new HashMap<>();

   // private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);

    public SSFlatRepeatVisit(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(String key, Iterator<Row> values, GroupState<Boolean> state) throws Exception {
        List<Row> tuples = new ArrayList<>();
        Row tuple;
        while (values.hasNext()) {
            incBoth();
            tuple = values.next();
            if (!state.exists()) {
                tuples.add(RowFactory.create(tuple.get(2), tuple.get(1), Boolean.TRUE, tuple.get(tuple.size() - 1)));
                state.update(true);
            } else {
                tuples.add(RowFactory.create(tuple.get(2), tuple.get(1), Boolean.FALSE, tuple.get(tuple.size() - 1)));
            }
        }
        return tuples.iterator();
    }
}