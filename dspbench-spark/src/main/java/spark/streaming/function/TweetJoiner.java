package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class TweetJoiner extends BaseFunction implements Function<Tuple2<Long, Tuple2<Tuple2<Tuple, Float>, Tuple2<Tuple, Float>>>, Tuple2<Long, Tuple>> {

    public TweetJoiner(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<Long, Tuple> call(Tuple2<Long, Tuple2<Tuple2<Tuple, Float>, Tuple2<Tuple, Float>>> t1) throws Exception {
        Tuple t = t1._2._1._1;
        t.set("positiveScore", t1._2._1._2);
        t.set("negativeScore", t1._2._2._2);
        
        //incReceived(2);
        //incEmitted();

        return new Tuple2<>(t1._1, t);
    }
}
