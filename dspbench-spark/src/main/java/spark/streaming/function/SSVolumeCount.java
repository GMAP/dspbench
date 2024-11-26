package spark.streaming.function;

import org.apache.commons.lang3.mutable.MutableLong;
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
public class SSVolumeCount extends BaseFunction implements MapGroupsWithStateFunction<Long, Row, MutableLong, Row> {

    public SSVolumeCount(Configuration config) {
        super(config);
    }

    @Override
    public Row call(Long key, Iterator<Row> values, GroupState<MutableLong> state) throws Exception {
        long inittime = 0;
        MutableLong count = new MutableLong();
        Row tuple;
        if (state.hasTimedOut())
            state.remove();

        while (values.hasNext()) {
            incBoth();
            tuple = values.next();
            inittime = tuple.getLong(tuple.size() - 1);

            if (state.exists()) {
                count = state.get();
            }
            count.increment();
            state.update(count);
        }
        return RowFactory.create(key, count.longValue(), inittime);
    }
}