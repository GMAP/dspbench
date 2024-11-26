package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.MachineOutlierConstants;
import spark.streaming.model.scorer.DataInstanceScorer;
import spark.streaming.model.scorer.DataInstanceScorerFactory;
import spark.streaming.model.scorer.ScorePackage;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
/**
 * @author luandopke
 */
public class SSObservationScore extends BaseFunction implements FlatMapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSObservationScore.class);
    private long previousTimestamp;
    private String dataTypeName;
    private DataInstanceScorer dataInstanceScorer;
    private List<Object> observationList;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);

    public SSObservationScore(Configuration config) {
        super(config);
        previousTimestamp = 0;
        dataTypeName = config.get(MachineOutlierConstants.Config.SCORER_DATA_TYPE);
        observationList = new ArrayList<>();
        dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
    }

    @Override
    public Iterator<Row> call(Row input) throws Exception {
        List<Row> tuples = new ArrayList<>();
        long timestamp = input.getLong(1);
        incReceived();
        //TODO: implements spark windowing method
        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                for (ScorePackage scorePackage : scorePackageList) {
                    incEmitted();
                    tuples.add(RowFactory.create(scorePackage.getId(), scorePackage.getScore(), previousTimestamp, scorePackage.getObj(), input.get(input.size() - 1)));
                }
                observationList.clear();
            }
            previousTimestamp = timestamp;
        }
        observationList.add(input.get(2));
        return tuples.iterator();
    }
}