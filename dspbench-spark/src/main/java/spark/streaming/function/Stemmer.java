package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import spark.streaming.model.NLPResource;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class Stemmer extends BaseFunction implements Function<Tuple2<Long, Tuple>, Tuple2<Long, Tuple>> {

    public Stemmer(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<Long, Tuple> call(Tuple2<Long, Tuple> t) throws Exception {
        String text = t._2.getString("text");

        for (String word : NLPResource.getStopWords()) {
            text = text.replaceAll("\\b" + word + "\\b", "");
        }
        
        t._2.set("text", text);
        //incBoth();
        return t;
    }
    
}
