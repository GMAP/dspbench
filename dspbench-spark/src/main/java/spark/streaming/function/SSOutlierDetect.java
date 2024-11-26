package spark.streaming.function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.HashMap;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;

import spark.streaming.util.Configuration;
import spark.streaming.util.collection.FixedMap;
import spark.streaming.util.math.OutlierTracker;

public class SSOutlierDetect extends BaseFunction implements FlatMapGroupsWithStateFunction<Long, Row, Long, Row>  {

    private static FixedMap<Long, Double> globalMedianBacklog;
    private static Map<String, OutlierTracker> outliers;
    private static PriorityQueue<ComparableTuple> unprocessedMessages;

    public SSOutlierDetect(Configuration config) {
        super(config);
    }

    private FixedMap<Long, Double>  med() {
        if (globalMedianBacklog == null) {
            globalMedianBacklog = new FixedMap<Long, Double>(300,300);
        }

        return globalMedianBacklog;
    }

    private Map<String, OutlierTracker>  outl() {
        if (outliers == null) {
            outliers = new HashMap<>();
        }

        return outliers;
    }

    private PriorityQueue<ComparableTuple>  msg() {
        if (unprocessedMessages == null) {
            unprocessedMessages = new PriorityQueue<>();
        }

        return unprocessedMessages;
    }

    @Override
    public Iterator<Row> call(Long key, Iterator<Row> values, GroupState<Long> state) throws Exception {
        List<Row> tuples = new ArrayList<>();
        Row tuple;

        med();
        outl();
        msg();

        while(values.hasNext()){
            incReceived();

            tuple = values.next();

            processPerPlugMedianTuple(tuple);
        }

        /*
        while(globalMedCalc.hasNext()){
            incReceived();

            tuple = globalMedCalc.next();

            long timestamp = tuple.getLong(2);
            double globalMedianLoad = tuple.getDouble(3);

            globalMedianBacklog.put(timestamp, globalMedianLoad);

            // ordered based on the timestamps
            while (!unprocessedMessages.isEmpty() && unprocessedMessages.peek().tuple.get(2).equals(timestamp)) {
                Row perPlugMedianTuple = unprocessedMessages.poll().tuple;
                Row dados = processPerPlugMedianTuple(perPlugMedianTuple);
                if (!(dados == null)){
                    incEmitted();
                    tuples.add(dados);
                }
            }
        } 
        */
        
        return tuples.iterator();
    }

    private Row processPerPlugMedianTuple(Row tuple) {

        String key     = tuple.getString(1);
        String houseId = key.split(":")[0];
        long timestamp = tuple.getLong(2);
        double value   = tuple.getDouble(3);
        
        if (globalMedianBacklog.containsKey(timestamp)) {
            OutlierTracker tracker;
            
            if (outliers.containsKey(houseId)) {
                tracker = outliers.get(houseId);
            } else {
                tracker = new OutlierTracker();
                outliers.put(houseId, tracker);
            }
            
            if (!tracker.isMember(key)) {
                tracker.addMember(key);
            }
            
            double globalMedian = globalMedianBacklog.get(timestamp);
            if (globalMedian < value) { // outlier
                if (!tracker.isOutlier(key)) {
                    tracker.addOutlier(key);
                    return RowFactory.create(timestamp - 24 * 60 * 60, timestamp, houseId, tracker.getCurrentPercentage());
                }
            } else {
                if (tracker.isOutlier(key)) {
                    tracker.removeOutlier(key);
                    //emit
                    return RowFactory.create(timestamp - 24 * 60 * 60, timestamp, houseId, tracker.getCurrentPercentage());
                }
            }
        } else {    // global median has not arrived
            unprocessedMessages.add(new ComparableTuple(tuple));
        }

        return null;
    }

    private class ComparableTuple implements Serializable, Comparable<ComparableTuple> {
        private final Row tuple;

        private ComparableTuple(Row tuple) {
            this.tuple = tuple;
        }

        @Override
        public int compareTo(ComparableTuple o) {
            Long field1 = this.tuple.getLong(2);
            Long field2 = o.tuple.getLong(2);
            return field1.compareTo(field2);
        }
    }

}