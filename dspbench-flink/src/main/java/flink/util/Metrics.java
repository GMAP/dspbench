package flink.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import flink.constants.BaseConstants;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Metrics implements Serializable{
    Configuration config;
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(150);
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    private File fileReceived;
    private File fileEmitted;
    private File pathTrh;
    private static final Logger LOG = LoggerFactory.getLogger(Metrics.class);

    private static MetricRegistry metrics;
    private Counter tuplesReceived;
    private Counter tuplesEmitted;

    private final Map<String, Long> received = new HashMap<>();
    private final Map<String, Long> emitted = new HashMap<>();

    public void initialize(Configuration config) {
        this.config = config;
        //getMetrics();
        //File pathLa = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK"), "latency").toFile();
        if(config.getBoolean(Configurations.METRICS_ENABLED, false)){
            this.pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK")).toFile(); //Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/gabriel/IDK"), "throughput").toFile();

            //pathLa.mkdirs();
            this.pathTrh.mkdirs();

            this.fileReceived = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), this.getClass().getSimpleName() + "-received.csv").toFile();
            this.fileEmitted = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), this.getClass().getSimpleName() + "-emitted.csv").toFile();
        }
    }

    public void initialize(Configuration config, String name) {
        this.config = config;
        //getMetrics();
        if(config.getBoolean(Configurations.METRICS_ENABLED, false)){
            if (!this.configPrefix.contains(name)) {
                this.configPrefix = String.format("%s.%s", configPrefix, name);
            }
            //File pathLa = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK"), "latency").toFile();
            this.pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK")).toFile(); // Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/gabriel/IDK"), "throughput").toFile();

            //pathLa.mkdirs();
            this.pathTrh.mkdirs();

            //this.file = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
            this.fileReceived = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), name + "-received.csv").toFile();
            this.fileEmitted = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), name + "-emitted.csv").toFile();
        }
    }

    public void calculateThroughput() {
        /*
        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(Configurations.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }
            Long ops = throughput.get(unixTime + "");
            if (ops == null) {
                for (Map.Entry<String, Long> entry : this.throughput.entrySet()) {
                    this.queue.add(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                }
                throughput.clear();
                if (queue.size() >= 10) {
                    SaveMetrics();
                }
            }

            ops = (ops == null) ? 1L : ++ops;

            throughput.put(unixTime + "", ops);
        }
         */
    }

    public void receiveThroughput() {
        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(Configurations.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
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
        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(Configurations.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
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
        
        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(Configurations.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
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

    /* 
    public void calculateLatency(long UnixTimeInit) {
        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            try {
                FileWriter fw = new FileWriter(Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "latency", this.getClass().getSimpleName() + this.configPrefix + ".csv").toFile(), true);
                fw.write(Instant.now().toEpochMilli() - UnixTimeInit + System.getProperty("line.separator"));
                fw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    */

    public void SaveMetrics() {
        this.pathTrh.mkdirs();
        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            new Thread(() -> {
                try {
                    try (Writer writer = new FileWriter(this.fileReceived, true)) {
                        for (Map.Entry<String, Long> entry : this.received.entrySet()) {
                            writer.append(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                        }
                        
                    } catch (IOException ex) {
                        System.out.println("Error while writing the file " + fileReceived + " - " + ex);
                    }
                } catch (Exception e) {
                    System.out.println("Error while creating the file " + e.getMessage());
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
    
    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(this.getClass().getSimpleName() + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(this.getClass().getSimpleName()+ "-emitted");
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
