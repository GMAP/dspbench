package spark.streaming.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.streaming.util.Configuration;

public class SSGlobalMedianCalc extends BaseFunction implements FlatMapFunction<Row, Row> {
    private static RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;

    private static final Logger LOG = LoggerFactory.getLogger(SSGlobalMedianCalc.class);

    public SSGlobalMedianCalc(Configuration config) {
        super(config);
    }

    private RunningMedianCalculator createMed(){
        if (medianCalc == null) {
            medianCalc = new RunningMedianCalculator();
        }

        return medianCalc;
    }

    @Override
    public Iterator<Row> call(Row t) throws Exception {
        incReceived();
        createMed();
        List<Row> tuples = new ArrayList<>();

        int operation  = t.getInt(5);
        double value   = t.getDouble(4);
        long timestamp = t.getLong(0);

        if (operation == 1){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTs = timestamp;
                incEmitted();
                tuples.add(RowFactory.create(timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }

        return tuples.iterator();
    }
    
}
