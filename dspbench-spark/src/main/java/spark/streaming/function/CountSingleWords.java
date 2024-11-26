package spark.streaming.function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class CountSingleWords extends BaseFunction implements PairFunction<Tuple2<String, Tuple>, String, Tuple> {

    public CountSingleWords(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<String, Tuple> call(Tuple2<String, Tuple> input) throws Exception {
        //incBoth();
        
        input._2.set("count", 1L);
        return input;
    }
}