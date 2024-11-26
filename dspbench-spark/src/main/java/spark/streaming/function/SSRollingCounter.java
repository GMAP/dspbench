package spark.streaming.function;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.streaming.constants.TrendingTopicsConstants;
import spark.streaming.tools.NthLastModifiedTimeTracker;
import spark.streaming.tools.SlidingWindowCounter;
import spark.streaming.util.Configuration;

public class SSRollingCounter extends BaseFunction implements FlatMapGroupsFunction<String, Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSRollingCounter.class);

    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
        "Actual window length is %d seconds when it should be %d seconds"
            + " (you can safely ignore this warning during the startup phase)";

    private SlidingWindowCounter<Object> counter;
    private int windowLengthInSeconds;
    private int emitFrequencyInSeconds;
    private NthLastModifiedTimeTracker lastModifiedTracker;
    
    public SSRollingCounter(Configuration config) {
        this(config, 60);
    }

    public SSRollingCounter(Configuration config, int emitFrequencyInSeconds) {
        super(config);

        this.emitFrequencyInSeconds = emitFrequencyInSeconds;

        windowLengthInSeconds = config.getInt(TrendingTopicsConstants.Config.COUNTER_WINDOW, 300);
        
        int numChunks = windowLengthInSeconds/emitFrequencyInSeconds;
        
        counter = new SlidingWindowCounter<>(numChunks);
        lastModifiedTracker = new NthLastModifiedTimeTracker(numChunks);
    }

    @Override
    public Iterator<Row> call(String key, Iterator<Row> values) throws Exception {
        Row tuple;
        List<Row> tuples = new ArrayList<>();
        while (values.hasNext()) {
            tuple = values.next();

            incReceived();

            String obj = tuple.getString(0);
            counter.incrementCount(obj);
        }

        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }

        for (Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            incEmitted();
            tuples.add(RowFactory.create(obj.toString(), count, actualWindowLengthInSeconds, new Timestamp(Instant.now().toEpochMilli())));
        }

        return tuples.iterator();
    }
    
}
