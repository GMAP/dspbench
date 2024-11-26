package spark.streaming.function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.streaming.model.NLPResource;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.Set;

/**
 *
 * @author mayconbordin
 */
public class SentimentTypeScorer extends BaseFunction implements PairFunction<Tuple2<Long, Tuple>, Long, Tuple2<Tuple, Float>> {
    public static enum Type {Positive, Negative}
    
    private final Type type;

    public SentimentTypeScorer(Type type, Configuration config, String name) {
        super(config, name);
        this.type = type;
    }
    
    public Set<String> getSentimentWords() {
        if (type == Type.Positive) {
            return NLPResource.getPosWords();
        } else {
            return NLPResource.getNegWords();
        }
    }
    
    @Override
    public Tuple2<Long, Tuple2<Tuple, Float>> call(Tuple2<Long, Tuple> t) throws Exception {
        String text = t._2.getString("text");
        String[] words = text.split(" ");
        
        int numWords = words.length;
        int numSentimentWords = 0;
        
        for (String word : words) {
            if (getSentimentWords().contains(word))
                numSentimentWords++;
        }
        
        //incBoth();
        return new Tuple2<>(t._1, new Tuple2<>(t._2, (float)numSentimentWords / numWords));
    }
    
}
