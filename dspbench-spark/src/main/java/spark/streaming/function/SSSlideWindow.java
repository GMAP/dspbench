package spark.streaming.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.shaded.org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple6;
import spark.streaming.constants.SmartGridConstants;
import spark.streaming.constants.SmartGridConstants.SlidingWindowAction;
import spark.streaming.model.window.SlidingWindow;
import spark.streaming.model.window.SlidingWindowCallback;
import spark.streaming.model.window.SlidingWindowEntry;
import spark.streaming.util.Configuration;

public class SSSlideWindow extends BaseFunction implements FlatMapFunction<Row, Row>  {
    private SlidingWindow window;

    public SSSlideWindow(Configuration config) {
        super(config);
        window = new SlidingWindow(1 * 60 * 60);
    }

    @Override
    public Iterator<Row> call(Row t) throws Exception {
        incReceived();
        List<Row> tuples = new ArrayList<>();
        
        int type = t.getInt(3);
        
        // we are interested only in load
        if (type == SmartGridConstants.Measurement.WORK) {
            return tuples.iterator();
        }

        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
                t.getLong(1), t.getDouble(2),
                t.getString(6), t.getString(5),
                t.getString(4));

        window.add(windowEntry, new SlidingWindowCallback() {
            @Override
            public void remove(List<SlidingWindowEntry> entries) {
                for (SlidingWindowEntry e : entries) {
                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
                    incEmitted();
                    tuples.add(RowFactory.create(entry.ts, entry.houseId, entry.houseHoldId, 
                        entry.plugId, entry.value, SlidingWindowAction.REMOVE));
                }
            }
        });
        incEmitted();
        tuples.add(RowFactory.create(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId,
            windowEntry.plugId, windowEntry.value, SlidingWindowAction.ADD));
        return tuples.iterator();
    }
    
    private class SlidingWindowEntryImpl implements SlidingWindowEntry {
        private final String houseId;
        private final String houseHoldId;
        private final String plugId;
        private final long ts;
        private final double value;

        private SlidingWindowEntryImpl(long ts, double value, String houseId, String houseHoldId, String plugId) {
            this.ts = ts;
            this.value = value;
            this.houseId = houseId;
            this.houseHoldId = houseHoldId;
            this.plugId = plugId;
        }

        @Override
        public long getTime() {
            return ts;
        }
    }
}
