package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.SentimentAnalysisConstants;
import spark.streaming.model.sentiment.SentimentClassifier;
import spark.streaming.model.sentiment.SentimentClassifierFactory;
import spark.streaming.model.sentiment.SentimentResult;
import spark.streaming.util.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
/**
 *
 * @author mayconbordin
 */
public class SSCalculateSentiment extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSCalculateSentiment.class);
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);
    private SentimentClassifier classifier;

    public SSCalculateSentiment(Configuration config) {
        super(config);

        String classifierType = config.get(SentimentAnalysisConstants.Config.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }

    @Override
    public Row call(Row input) throws Exception {
        String tweetId = input.getString(0);
        String text = input.getString(1);
        String timestamp = new DateTime(input.get(2)).toString();

        SentimentResult result = classifier.classify(text);
        incBoth();
        return RowFactory.create(tweetId,
                text,
                timestamp,
                result.getSentiment().toString(),
                result.getScore());
    }
}