package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.model.window.SlidingWindow;
import spark.streaming.model.window.SlidingWindowEntry;
import spark.streaming.util.Configuration;

/**
 * @author luandopke
 */
public class SSSlidingWindow extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSSlidingWindow.class);

    public SSSlidingWindow(Configuration config) {
        super(config);
        window = new SlidingWindow(1 * 60 * 60);
    }

    private SlidingWindow window;

    @Override
    public Row call(Row value) throws Exception {
//        int type = tuple.getIntegerByField(Field.PROPERTY);
//
//        // we are interested only in load
//        if (type == SmartGridConstants.Measurement.WORK) {
//            return null;
//        }
//
//        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
//                tuple.getLongByField(Field.TIMESTAMP), tuple.getDoubleByField(Field.VALUE),
//                tuple.getStringByField(Field.HOUSE_ID), tuple.getStringByField(Field.HOUSEHOLD_ID),
//                tuple.getStringByField(Field.PLUG_ID));
//
//        window.add(windowEntry, new SlidingWindowCallback() {
//            @Override
//            public void remove(List<SlidingWindowEntry> entries) {
//                for (SlidingWindowEntry e : entries) {
//                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
//                    collector.emit(new Values(entry.ts, entry.houseId, entry.houseHoldId,
//                            entry.plugId, entry.value, SlidingWindowAction.REMOVE));
//                }
//            }
//        });
//
//        collector.emit(new Values(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId,
//                windowEntry.plugId, windowEntry.value, SlidingWindowAction.ADD, tuple.getStringByField(Field.INITTIME)));
//
//        return RowFactory.create(deviceID, movingAverageInstant, nextDouble, "spike detected", value.get(value.size() - 1));

        return null;
    }

    private class SlidingWindowEntryImpl implements SlidingWindowEntry {
        private String houseId;
        private String houseHoldId;
        private String plugId;
        private long ts;
        private double value;

        private SlidingWindowEntryImpl(long ts, double value, String houseId,
                                       String houseHoldId, String plugId) {
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