package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.util.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSSpikeDetector extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSSpikeDetector.class);
    private double spikeThreshold;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue = new ArrayBlockingQueue<>(20);

    public SSSpikeDetector(Configuration config) {
        super(config);
        spikeThreshold = config.getDouble(SpikeDetectionConstants.Config.SPIKE_DETECTOR_THRESHOLD, 0.03d);
    }

    @Override
    public Row call(Row value) throws Exception {
        incReceived();
        int deviceID = value.getInt(0);
        double movingAverageInstant = value.getDouble(1);
        double nextDouble = value.getDouble(2);

        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            incEmitted();
            return RowFactory.create(deviceID, movingAverageInstant, nextDouble, "spike detected");
        }
        return null;
    }
}