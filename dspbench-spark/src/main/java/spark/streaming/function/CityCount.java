package spark.streaming.function;

import org.apache.spark.api.java.function.Function2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class CityCount extends BaseFunction implements Function2<Tuple, Tuple, Tuple> {

    public CityCount(Configuration config) {
        super(config);
    }

    @Override
    public Tuple call(Tuple tupleOne, Tuple tupleTwo) throws Exception {
        //incReceived(2);
        
        if (tupleOne == null && tupleTwo == null) return null;
        if (tupleOne == null) return tupleTwo;
        if (tupleTwo == null) return tupleOne;
        
        Tuple newTuple = new Tuple(tupleOne, tupleTwo);
        newTuple.set("count", (long)tupleOne.get("count") + (long)tupleTwo.get("count"));
        //incEmitted();
        
        return newTuple;
    }
}