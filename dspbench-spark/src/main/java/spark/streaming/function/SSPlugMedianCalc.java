package spark.streaming.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;

import spark.streaming.util.Configuration;

public class SSPlugMedianCalc extends BaseFunction implements FlatMapGroupsWithStateFunction<String, Row, String, Row> {
    private static Map<String, RunningMedianCalculator> runningMedians;
    private static Map<String, Long> lastUpdatedTsMap;

    public SSPlugMedianCalc(Configuration config) {
        super(config);
    }

    private Map<String, RunningMedianCalculator> runMed(){
        if (runningMedians == null) {
            runningMedians = new HashMap<>();
        }

        return runningMedians;
    }

    private Map<String, Long> tsMap(){
        if (lastUpdatedTsMap == null) {
            lastUpdatedTsMap = new HashMap<>();
        }

        return lastUpdatedTsMap;
    }

    @Override
    public Iterator<Row> call(String key, Iterator<Row> values, GroupState<String> state) throws Exception {
        List<Row> tuples = new ArrayList<>();
        Row tuple;
        runMed();
        tsMap();

        while (values.hasNext()) {
            incReceived();
            tuple = values.next();
            
            int operation = tuple.getInt(5);
            double value = tuple.getDouble(4);
            long timestamp = tuple.getLong(0);

            RunningMedianCalculator medianCalc = runningMedians.get(key);
            if (medianCalc == null) {
                medianCalc = new RunningMedianCalculator();
                runningMedians.put(key, medianCalc);
            }

            Long lastUpdatedTs = lastUpdatedTsMap.get(key);
            if (lastUpdatedTs == null) {
                lastUpdatedTs = 0L;
            }

            if (operation == 1) {
                double median = medianCalc.getMedian(value);
                if (lastUpdatedTs < timestamp) {
                    // the sliding window has moved
                    lastUpdatedTsMap.put(key, timestamp);
                    incEmitted();
                    tuples.add(RowFactory.create(key, timestamp, median));
                }
            } else {
                medianCalc.remove(value);
            }
        }

        return tuples.iterator();
    }
    
}
