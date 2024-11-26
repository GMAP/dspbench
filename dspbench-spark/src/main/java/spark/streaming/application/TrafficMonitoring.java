package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.TrafficMonitoringConstants;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.function.SSBeijingTaxiTraceParser;
import spark.streaming.function.SSFilterNull;
import spark.streaming.function.SSMapMatcher;
import spark.streaming.function.SSSpeedCalculator;
import spark.streaming.model.gis.Road;
import spark.streaming.util.Configuration;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark.streaming.util.Tuple;

import java.beans.Encoder;
import java.util.HashMap;
import java.util.Map;

public class TrafficMonitoring extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    private int parserThreads;
    private int mapMatcherThreads;
    private int speedCalculatorThreads;

    public TrafficMonitoring(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(TrafficMonitoringConstants.Config.PARSER_THREADS, 1);
        mapMatcherThreads = config.getInt(TrafficMonitoringConstants.Config.MAP_MATCHER_THREADS, 1);
        speedCalculatorThreads = config.getInt(TrafficMonitoringConstants.Config.SPEED_CALCULATOR_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() {
        StructType schema = new StructType(new StructField[]{
                new StructField("carId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("occ", DataTypes.BooleanType, true, Metadata.empty()),
                new StructField("lat", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("lon", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("speed", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("bearing", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("inittime", DataTypes.LongType, false, Metadata.empty())
        });

        Dataset<Row> rawRecords = createSource();

        Dataset<Row> records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSBeijingTaxiTraceParser(config), Encoders.row(schema));

        KeyValueGroupedDataset<Integer, Row> roads = records.filter(new SSFilterNull<>())
                .repartition(mapMatcherThreads)
                .groupByKey(new SSMapMatcher(config), Encoders.INT());

        Dataset<Row> speed = roads
                .mapGroupsWithState(new SSSpeedCalculator(config), Encoders.kryo(Road.class), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout())
                .repartition(speedCalculatorThreads);

        return createSink(speed);
    }

    @Override
    public String getConfigPrefix() {
        return TrafficMonitoringConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
