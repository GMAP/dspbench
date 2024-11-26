package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;
import spark.streaming.model.MachineMetadata;
import spark.streaming.util.Configuration;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSAlibabaMachineUsageParser extends BaseFunction implements MapFunction<String, Row> {
    private static final int TIMESTAMP = 1;
    private static final int MACHINE_ID = 0;
    private static final int CPU = 2;
    private static final int MEMORY = 3;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);


    public SSAlibabaMachineUsageParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String value) throws Exception {
        incReceived();
        String[] items = value.split(",");

        if (items.length != 9)
            return null;

        String id = items[MACHINE_ID];
        long timestamp = Long.parseLong(items[TIMESTAMP]) * 1000;
        double cpu = Double.parseDouble(items[CPU]);
        double memory = Double.parseDouble(items[MEMORY]);
        incEmitted();
        return RowFactory.create(id,
                timestamp,
                new MachineMetadata(timestamp, id, cpu, memory)
        );
    }
}