package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class TweetParser extends BaseFunction implements Function<Tuple2<String, Tuple>, Tuple2<Long, Tuple>> {
    private transient JSONParser parser;

    public TweetParser(Configuration config) {
        super(config);
    }

    private JSONParser getParser() {
        if (parser == null) {
            parser = new JSONParser();
        }
        return parser;
    }
    
    @Override
    public Tuple2<Long, Tuple> call(Tuple2<String, Tuple> t) throws Exception {
        JSONObject obj = (JSONObject) getParser().parse(t._1);

        long id = (long) obj.get("id");
        t._2.set("lang", obj.get("lang"));
        t._2.set("text", obj.get("text"));
        
        //incBoth();
        return new Tuple2<>(id, t._2);
    }

}