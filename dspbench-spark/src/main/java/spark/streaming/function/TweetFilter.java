package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class TweetFilter extends BaseFunction implements Function<Tuple2<Long, Tuple>, Boolean> {

    public TweetFilter(Configuration config) {
        super(config);
    }

    @Override
    public Boolean call(Tuple2<Long, Tuple> input) throws Exception {
        boolean res = input._2.getString("lang") != null && input._2.getString("lang").equals("en");
        
        //incReceived();
        //if (res) incEmitted();
        
        return res;
    }
    
}
