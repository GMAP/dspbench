package spark.streaming;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.application.*;
import spark.streaming.util.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility class to run a Spark Streaming application
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StruturedStreamingRunner {
    private static final Logger LOG = LoggerFactory.getLogger(StruturedStreamingRunner.class);
    private static final String CFG_PATH = "/config/%s.properties";

    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = {"-m", "--master"}, description = "Cluster URL for Spark, Mesos or YARN")
    public String master = "local[2]";

    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = true)
    public String application;

    @Parameter(names = {"-n", "--application-name"}, required = false, description = "The name of the topology")
    public String applicationName;

    @Parameter(names = {"--config"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;

    @Parameter(names = {"-r", "--runtime"}, required = false, description = "Runtime in seconds for the application")
    public Integer timeoutInSeconds;

    private final AppDriver driver;
    private Configuration config;

    public StruturedStreamingRunner() {
        driver = new AppDriver();

        driver.addApp("wordcount", WordCount.class);
        // driver.addApp("log-processing", LogProcessing.class);
        driver.addApp("trafficmonitoring", TrafficMonitoring.class);
        driver.addApp("sentimentanalysis", SentimentAnalysis.class);
        driver.addApp("frauddetection", FraudDetection.class);
        driver.addApp("spikedetection", SpikeDetection.class);
        driver.addApp("clickanalytics", ClickAnalytics.class);
        driver.addApp("machineoutlier", MachineOutlier.class);
        driver.addApp("logprocessing", LogProcessing.class);
        driver.addApp("smartgrid", SmartGrid.class);
        driver.addApp("trendingtopics", TrendingTopics.class);
    }


    public void run() throws InterruptedException {
        // Loads the configuration file set by the user or the default configuration
        try {
            // load default configuration
            if (configStr == null) {
                String cfg = String.format(CFG_PATH, application);
                Properties p = loadProperties(cfg, true);

                config = Configuration.fromProperties(p);
                LOG.info("Loaded default configuration file {}", cfg);
            } else {
                config = Configuration.fromStr(configStr);
                LOG.info("Loaded configuration from command line argument");
            }
        } catch (IOException ex) {
            LOG.error("Unable to load configuration file", ex);
            throw new RuntimeException("Unable to load configuration file", ex);
        }

        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // In case no topology names is given, create one
        if (applicationName == null) {
            applicationName = String.format("%s-%d", application, new Random().nextInt());
        }

        config.setAppName(applicationName);
        config.setMaster(master);

        DataStreamWriter<Row> query = app.getContext(applicationName, config);
        try {
            if (query != null) {
                if (timeoutInSeconds != null) {
                    query.start().awaitTermination(TimeUnit.SECONDS.toMillis(timeoutInSeconds));
                } else {
                    query.start().awaitTermination();
                }
            }
        } catch (TimeoutException | StreamingQueryException e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) throws Exception {
        StruturedStreamingRunner runner = new StruturedStreamingRunner();
        JCommander cmd = new JCommander(runner);

        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
            System.exit(1);
        }

        runner.run();
    }

    public static Properties loadProperties(String filename, boolean classpath) throws IOException {
        Properties properties = new Properties();
        InputStream is;

        if (classpath) {
            is = StruturedStreamingRunner.class.getResourceAsStream(filename);
        } else {
            is = new FileInputStream(filename);
        }

        properties.load(is);
        is.close();

        return properties;
    }
}
