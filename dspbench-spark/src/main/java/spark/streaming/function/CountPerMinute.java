package spark.streaming.function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class CountPerMinute extends BaseFunction implements PairFunction<Tuple2<Long, Tuple>, Long, Tuple> {

    public CountPerMinute(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<Long, Tuple> call(Tuple2<Long, Tuple> input) throws Exception {
        //incBoth();
        
        Tuple t = new Tuple(input._2);
        t.set("count", 1L);
        
        long ts_min = (long) input._2.get("timestamp_minutes");
        
        return new Tuple2<>(ts_min, t);
    }
}