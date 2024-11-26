package spark.streaming.function;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import org.apache.hadoop.shaded.org.apache.commons.configuration2.builder.fluent.Configurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.metrics.MetricsFactory;
import spark.streaming.util.Configuration;

import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

/**
 * @author mayconbordin
 */
public abstract class BaseFunction implements Serializable {
    private String name;
    private Integer id;
    private static MetricRegistry metrics;
    private Counter tuplesReceived;
    private Counter tuplesEmitted;
    private Configuration config;
    private String configStr;
    private static final Logger LOG = LoggerFactory.getLogger(BaseFunction.class);

    protected String configPrefix;
    private File fileReceived;
    private File fileEmitted;
    private File pathTrh;
    private final Map<String, Long> received = new HashMap<>();
    private final Map<String, Long> emitted = new HashMap<>();

    public BaseFunction(Configuration config) {
        this();
        this.configStr = config.toString();
        this.config = config;
        if (config.getBoolean(config.METRICS_ENABLED, false)) {
            this.pathTrh = Paths.get(config.get(Configuration.METRICS_OUTPUT,"/home/IDK")).toFile();
            
            this.pathTrh.mkdirs();

            //this.fileReceived = Paths.get(config.get(Configuration.METRICS_OUTPUT, "/home/IDK"), this.getClass().getSimpleName() + "-received.csv").toFile();
            //this.fileEmitted = Paths.get(config.get(Configuration.METRICS_OUTPUT, "/home/IDK"), this.getClass().getSimpleName() + "-emitted.csv").toFile();
           // this.file = Paths.get(config.get(Configuration.METRICS_OUTPUT), "throughput", this.getClass().getSimpleName() + ".csv").toFile();
        }
    }

    public BaseFunction(Configuration config, String name) {
        this.name = name;
        this.configStr = config.toString();
        if (config.getBoolean(config.METRICS_ENABLED, false)) {
            if (!this.configPrefix.contains(name)) {
                this.configPrefix = String.format("%s.%s", configPrefix, name);
            }
            this.pathTrh = Paths.get(config.get(Configuration.METRICS_OUTPUT,"/home/IDK")).toFile();
            
            this.pathTrh.mkdirs();

            //this.fileReceived = Paths.get(config.get(Configuration.METRICS_OUTPUT, "/home/IDK"), name + "-received.csv").toFile();
            //this.fileEmitted = Paths.get(config.get(Configuration.METRICS_OUTPUT, "/home/IDK"), name + "-emitted.csv").toFile();
        }
    }

    public BaseFunction(String name) {
        this.name = name;
    }

    public BaseFunction() {
        this.name = this.getClass().getSimpleName();
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

    public void emittedThroughput() {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.get(Configuration.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }
            Long emit = emitted.get(unixTime + "");

            emit = (emit == null) ? 1L : ++emit;
            emitted.put(unixTime + "", emit);
        }
    }

    public void recemitThroughput() {   
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.get(Configuration.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }
            Long rec = received.get(unixTime + "");
            Long emit = emitted.get(unixTime + "");

            rec = (rec == null) ? 1L : ++rec;
            received.put(unixTime + "", rec);

            emit = (emit == null) ? 1L : ++emit;
            emitted.put(unixTime + "", emit);
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
                if (!this.configPrefix.contains("Sink")) {
                    try {
                        try (Writer writer = new FileWriter(this.fileEmitted, true)) {
                            for (Map.Entry<String, Long> entry : this.emitted.entrySet()) {
                                writer.append(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                            }
                        } catch (IOException ex) {
                            System.out.println("Error while writing the file " + fileEmitted + " - " + ex);
                        }
                    } catch (Exception e) {
                        System.out.println("Error while creating the file " + e.getMessage());
                    }
                }
            }).start();
        }
    }

    public void setConfiguration(String cfg) {
        configStr = cfg;
    }

    public Configuration getConfiguration() {
        if (config == null) {
            config = Configuration.fromStr(configStr);
        }

        return config;
    }

    protected int getId() {
        if (id == null) {
            id = Math.abs(UUID.randomUUID().hashCode());
        }
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

     
    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            this.pathTrh.mkdirs();
            metrics = MetricsFactory.createRegistry(getConfiguration());
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(name + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(name+ "-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived() {
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
}
