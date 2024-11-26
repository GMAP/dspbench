package spark.streaming.function;

import org.apache.spark.api.java.function.Function2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class CountStatus extends BaseFunction implements Function2<Tuple, Tuple, Tuple> {

    public CountStatus(Configuration config) {
        super(config);
    }

    @Override
    public Tuple call(Tuple tupleOne, Tuple tupleTwo) throws Exception {
        //incReceived(2);
        
        Tuple newTuple = new Tuple(tupleOne, tupleTwo);
        newTuple.set("count", (long)tupleOne.get("count") + (long)tupleTwo.get("count"));
        //incEmitted();
        
        return newTuple;
    }
}