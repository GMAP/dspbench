package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.streaming.constants.SmartGridConstants;
import spark.streaming.function.*;
import spark.streaming.util.Configuration;

public class SmartGrid extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SmartGrid.class);

    private int parserThreads;
    private int slideWindowThreads;
    private int globalMedianThreads;
    private int plugMedianThreads;
    private int outlierDetectorThreads;
    private int houseLoadThreads;
    private int plugLoadThreads;
    private int houseLoadFrequency;
    private int plugLoadFrequency;

    public SmartGrid(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(SmartGridConstants.Config.PARSER_THREADS, 1);
        slideWindowThreads = config.getInt(SmartGridConstants.Config.SLIDING_WINDOW_THREADS, 1);
        globalMedianThreads = config.getInt(SmartGridConstants.Config.GLOBAL_MEDIAN_THREADS, 1);
        plugMedianThreads = config.getInt(SmartGridConstants.Config.PLUG_MEDIAN_THREADS, 1);
        outlierDetectorThreads = config.getInt(SmartGridConstants.Config.OUTLIER_DETECTOR_THREADS, 1);
        houseLoadThreads = config.getInt(SmartGridConstants.Config.HOUSE_LOAD_THREADS, 1);
        plugLoadThreads = config.getInt(SmartGridConstants.Config.PLUG_LOAD_THREADS, 1);

        houseLoadFrequency = config.getInt(SmartGridConstants.Config.HOUSE_LOAD_FREQUENCY, 15);
        plugLoadFrequency = config.getInt(SmartGridConstants.Config.PLUG_LOAD_FREQUENCY, 15);
    }

    @Override
    public DataStreamWriter buildApplication() throws StreamingQueryException {
       
        //Source
        Dataset<Row> rawRecords = createSource();

        //Parser
        Dataset<Row> records = rawRecords
            .repartition(parserThreads)
            .as(Encoders.STRING())
            .map(new SSSmartGridParser(config), Encoders.kryo(Row.class));

        //Process
        Dataset<Row> slideWindow = records.repartition(slideWindowThreads)
            .filter(new SSFilterNull<>())
            .flatMap(new SSSlideWindow(config),  Encoders.kryo(Row.class));

        Dataset<Row> globalMedCalc = slideWindow.repartition(globalMedianThreads)
            .filter(new SSFilterNull<>())
            .flatMap(new SSGlobalMedianCalc(config),  Encoders.kryo(Row.class));
        
        Dataset<Row> plugMedCalc = slideWindow.repartition(plugMedianThreads)
            .filter(new SSFilterNull<>())
            .groupByKey((MapFunction<Row, String>) row -> row.get(1) + ":" + row.get(2) + ":" + row.get(3), Encoders.STRING())
            .flatMapGroupsWithState(new SSPlugMedianCalc(config), OutputMode.Append(), Encoders.STRING(), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

        Dataset<Row> outlierDetect;
            
        
        //plugMedCalc.repartition(outlierDetectorThreads)
        //    .groupByKey((MapFunction<Row, Long>) row -> row.getLong(1), Encoders.LONG())
        //    .flatMapGroupsWithState(new SSOutlierDetect(config), OutputMode.Append(), Encoders.LONG(), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

        //Dataset<Row> houseLoadPredictor = records.repartition(houseLoadThreads)
        //    .groupByKey((MapFunction<Row, Long>) row -> row.getLong(6), Encoders.LONG())
        //    .
       
        //Dataset<Row> plugLoadPredictor;

        //Sink        
        return createSink(records);
    }

    @Override
    public String getConfigPrefix() {
        return SmartGridConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
