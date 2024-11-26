package spark.streaming.function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class CountSingleStatus extends BaseFunction implements PairFunction<Tuple2<Long, Tuple>, Integer, Tuple> {

    public CountSingleStatus(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<Integer, Tuple> call(Tuple2<Long, Tuple> input) throws Exception {
        //incBoth();
        
        Tuple t = new Tuple(input._2);
        t.set("count", 1L);
        
        int status = (int) input._2.get("response");
        
        return new Tuple2<>(status, t);
    }
}