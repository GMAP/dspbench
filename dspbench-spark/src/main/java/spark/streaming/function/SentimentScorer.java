package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class SentimentScorer extends BaseFunction implements Function<Tuple2<Long, Tuple>, Tuple2<Long, Tuple>> {

    public SentimentScorer(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<Long, Tuple> call(Tuple2<Long, Tuple> t1) throws Exception {
        //incBoth();
        Float pos = t1._2.getFloat("positiveScore");
        Float neg = t1._2.getFloat("negativeScore");
        
        String score = pos > neg ? "positive" : "negative";
        t1._2.set("score", score);
        return t1;
    }
    
}
