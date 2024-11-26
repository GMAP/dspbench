package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import scala.Tuple2;
import spark.streaming.model.VisitStats;
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
public class SSVisitStats extends BaseFunction implements FlatMapGroupsWithStateFunction<Integer, Row, VisitStats, Row> {

    public SSVisitStats(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(Integer key, Iterator<Row> values, GroupState<VisitStats> state) throws Exception {
        VisitStats stats;
        Row tuple;
        List<Row> tuples = new ArrayList<>();
        while (values.hasNext()) {
            incBoth();
            tuple = values.next();
            if (!state.exists()) {
                stats = new VisitStats();
            } else {
                stats = state.get();
            }
            stats.add(tuple.getBoolean(2));
            state.update(stats);
            tuples.add(RowFactory.create(stats.getTotal(), stats.getUniqueCount(), tuple.get(tuple.size() - 1)));
        }
        return tuples.iterator();
    }
}