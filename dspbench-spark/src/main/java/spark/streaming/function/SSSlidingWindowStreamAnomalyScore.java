package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.MachineOutlierConstants;
import spark.streaming.util.Configuration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSSlidingWindowStreamAnomalyScore extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSSlidingWindowStreamAnomalyScore.class);

    private Map<String, Queue<Double>> slidingWindowMap;
    private int windowLength;
    private long previousTimestamp;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);


    public SSSlidingWindowStreamAnomalyScore(Configuration config) {
        super(config);
        windowLength = config.getInt(MachineOutlierConstants.Config.ANOMALY_SCORER_WINDOW_LENGTH, 10);
        slidingWindowMap = new HashMap<>();
        previousTimestamp = 0;
    }

    @Override
    public Row call(Row input) throws Exception {
        long timestamp = input.getLong(2); //TODO change to window spark method
        String id = input.getString(0);
        double dataInstanceAnomalyScore = input.getDouble(1);

        Queue<Double> slidingWindow = slidingWindowMap.get(id);
        if (slidingWindow == null) {
            slidingWindow = new LinkedList<>();
        }

        // update sliding window
        slidingWindow.add(dataInstanceAnomalyScore);
        if (slidingWindow.size() > this.windowLength) {
            slidingWindow.poll();
        }
        slidingWindowMap.put(id, slidingWindow);

        double sumScore = 0.0;
        for (double score : slidingWindow) {
            sumScore += score;
        }
        incBoth();
        return RowFactory.create(id, sumScore, timestamp, input.get(3), dataInstanceAnomalyScore, input.get(input.size() - 1));
    }
}