package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import scala.Tuple2;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.model.Moving;
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
public class SSFlatMovingAverage extends BaseFunction implements FlatMapGroupsWithStateFunction<Integer, Row, Moving, Row> {
    private int movingAverageWindow;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);

    public SSFlatMovingAverage(Configuration config) {
        super(config);
        movingAverageWindow = config.getInt(SpikeDetectionConstants.Config.MOVING_AVERAGE_WINDOW, 1000);
    }

    @Override
    public Iterator<Row> call(Integer key, Iterator<Row> values, GroupState<Moving> state) throws Exception {
        List<Row> tuples = new ArrayList<>();
        double value = 0, avg = 0;
        Moving mov;
        Row tuple;
        while (values.hasNext()) {
            incBoth();
            tuple = values.next();
            value = tuple.getDouble(2);
            avg = value;

            if (!state.exists()) {
                mov = new Moving(key);
                mov.add(value);
            } else {
                mov = state.get();

                if (mov.getList().size() > movingAverageWindow - 1) {
                    mov.remove();
                }
                mov.add(value);
                avg = mov.getSum() / mov.getList().size();
            }

            state.update(mov);
            tuples.add(RowFactory.create(key, avg, value));
        }
        return tuples.iterator();
    }
}