package spark.streaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import spark.streaming.constants.BaseConstants;

public class ConsoleSink extends BaseSink {
    @Override
    public DataStreamWriter<Row> sinkStream(Dataset<Row> dt) { //, Configuration conf
        return dt.writeStream().foreach(new ForeachWriter<Row>() {

                    @Override
                    public boolean open(long partitionId, long version) {
                        return true;
                    }

                    @Override
                    public void process(Row value) {
                        System.out.println(value);
                        //receiveThroughput();
                        incReceived();
                        /*if (value != null)
                            calculateLatency(value.getLong(value.size() - 1));*/
                    } //TODO make formater as field=value,

                    @Override
                    public void close(Throwable errorOrNull) {
                        // Close the connection
                    }
                }).outputMode(config.get(BaseConstants.BaseConfig.OUTPUT_MODE, "update"))
                .trigger(Trigger.AvailableNow());
    }
}
