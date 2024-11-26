package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.util.BFPRT;
import spark.streaming.util.Configuration;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSAlertTrigger extends BaseFunction implements FlatMapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSAlertTrigger.class);
    private static final double dupper = Math.sqrt(2);
    private long previousTimestamp;
    private List<Row> streamList;
    private double minDataInstanceScore = Double.MAX_VALUE;
    private double maxDataInstanceScore = 0;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue = new ArrayBlockingQueue<>(20);;
    public SSAlertTrigger(Configuration config) {
        super(config);
        previousTimestamp = 0;
        streamList = new ArrayList<>();
    }

    @Override
    public Iterator<Row> call(Row input) throws Exception {
        long timestamp = input.getLong(2);
        List<Row> tuples = new ArrayList<>();
        incReceived();
        if (timestamp > previousTimestamp) {
            // new batch of stream scores
            if (!streamList.isEmpty()) {
                List<Row> abnormalStreams = this.identifyAbnormalStreams();
                int medianIdx = (int) streamList.size() / 2;
                double minScore = abnormalStreams.get(0).getDouble(1);
                double medianScore = abnormalStreams.get(medianIdx).getDouble(1);

                for (Row streamProfile : abnormalStreams) {
                    double streamScore = streamProfile.getDouble(1);
                    double curDataInstScore = streamProfile.getDouble(4);
                    boolean isAbnormal = false;

                    // current stream score deviates from the majority
                    if ((streamScore > 2 * medianScore - minScore) && (streamScore > minScore + 2 * dupper)) {
                        // check whether cur data instance score return to normal
                        if (curDataInstScore > 0.1 + minDataInstanceScore) {
                            isAbnormal = true;
                        }
                    }

                    if (isAbnormal) {
                        incEmitted();
                        tuples.add(RowFactory.create(streamProfile.getString(0), streamScore, streamProfile.getLong(2), isAbnormal, streamProfile.get(3), input.get(input.size() - 1)));
                    }
                }

                streamList.clear();
                minDataInstanceScore = Double.MAX_VALUE;
                maxDataInstanceScore = 0;
            }
            previousTimestamp = timestamp;
        }

        double dataInstScore = input.getDouble(4);
        if (dataInstScore > maxDataInstanceScore) {
            maxDataInstanceScore = dataInstScore;
        }

        if (dataInstScore < minDataInstanceScore) {
            minDataInstanceScore = dataInstScore;
        }

        streamList.add(input);
        return tuples.iterator();
    }

    private List<Row> identifyAbnormalStreams() {
        List<Row> abnormalStreamList = new ArrayList<>();
        int medianIdx = (int) (streamList.size() / 2);
        BFPRT.bfprt(streamList, medianIdx);
        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}