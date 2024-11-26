package spark.streaming.sink;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.function.BaseFunction;
import spark.streaming.metrics.MetricsFactory;
import spark.streaming.util.Configuration;

import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class BaseSink implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(BaseSink.class);
    protected Configuration config;
    protected transient SparkSession session;
    private String sinkName;
    private static MetricRegistry metrics;
    private  Counter tuplesReceived;
    private Counter tuplesEmitted;

    private File fileReceived;
    private File pathTrh;
    private final Map<String, Long> received = new HashMap<>();
    

    public void initialize(Configuration config, SparkSession session) {
        initialize(config, session, "sink");
    }

    public void initialize(Configuration config, SparkSession session, String sinkName) {
        this.config = config;
        this.session = session;
        this.sinkName = sinkName;
        if (config.getBoolean(config.METRICS_ENABLED, false)) {
            this.pathTrh = Paths.get(config.get(Configuration.METRICS_OUTPUT,"/home/IDK")).toFile();
            
            this.pathTrh.mkdirs();

            //this.fileReceived = Paths.get(config.get(Configuration.METRICS_OUTPUT, "/home/IDK"), this.getClass().getSimpleName() + "-received.csv").toFile();
        }
    }

    public abstract DataStreamWriter<Row> sinkStream(Dataset<Row> dt);

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(this.config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(sinkName + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(sinkName+ "-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived() {
        getTuplesReceived().inc();
    }

    protected void incReceived(Configuration config) {
        this.config = config;
        getTuplesReceived().inc();
    }

    protected void incReceived(long n) {
        getTuplesReceived().inc(n);
    }

    protected void incEmitted() {
        getTuplesEmitted().inc();
    }

    protected void incEmitted(long n) {
        getTuplesEmitted().inc(n);
    }

    protected void incBoth() {
        getTuplesReceived().inc();
        getTuplesEmitted().inc();
    }
    
    public String getName() {
        return sinkName;
    }

    public void receiveThroughput() {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.get(Configuration.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }
            Long rec = received.get(unixTime + "");

            rec = (rec == null) ? 1L : ++rec;
            received.put(unixTime + "", rec);
        }
    }

    public void SaveMetrics() {
        this.pathTrh.mkdirs();
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            new Thread(() -> {
                try {
                    try (Writer writer = new FileWriter(this.fileReceived, true)) {
                        for (Map.Entry<String, Long> entry : this.received.entrySet()) {
                            writer.append(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                        }
                    } catch (IOException ex) {
                        LOG.error("Error while writing the file " + this.fileReceived, ex);
                    }
                } catch (Exception e) {
                    LOG.error("Error while creating the file " + e.getMessage());
                }
            }).start();
        }
    }
}
