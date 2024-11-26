package spark.streaming.application;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.ClickAnalyticsConstants;
import spark.streaming.constants.LogProcessingConstants;
import spark.streaming.function.*;
import spark.streaming.model.CountryStats;
import spark.streaming.model.VisitStats;
import spark.streaming.util.Configuration;

public class LogProcessing extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(LogProcessing.class);
    private int parserThreads;
    private int volumeCountThreads;
    private int statusCountThreads;
    private int geographyThreads;
    private int geoStatsThreads;
    private String volumeSink;
    private String statusSink;
    private String geoSink;

    public LogProcessing(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(LogProcessingConstants.Config.PARSER_THREADS, 1);
        volumeCountThreads = config.getInt(LogProcessingConstants.Config.VOLUME_COUNTER_THREADS, 1);
        statusCountThreads = config.getInt(LogProcessingConstants.Config.STATUS_COUNTER_THREADS, 1);
        geographyThreads = config.getInt(LogProcessingConstants.Config.GEO_FINDER_THREADS, 1);
        geoStatsThreads = config.getInt(LogProcessingConstants.Config.GEO_STATS_THREADS, 1);

        volumeSink = config.get(LogProcessingConstants.Component.VOLUME_SINK);
        statusSink = config.get(LogProcessingConstants.Component.STATUS_SINK);
        geoSink = config.get(LogProcessingConstants.Component.GEO_SINK);
    }

    @Override
    public DataStreamWriter buildApplication() throws StreamingQueryException {
        StructType schema = new StructType(new StructField[]{
                new StructField("ip", DataTypes.StringType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("minute", DataTypes.LongType, true, Metadata.empty()),
                new StructField("request", DataTypes.StringType, true, Metadata.empty()),
                new StructField("response", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("bytesize", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("inittime", DataTypes.LongType, false, Metadata.empty())
        });


        Dataset<Row> rawRecords = createSource();

        Dataset<Row> records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSCommonLogParser(config), Encoders.row(schema));

        Dataset<Row> volumeCount = records
                .repartition(volumeCountThreads)
                .withWatermark("timestamp", "1 hour")
                .groupByKey((MapFunction<Row, Long>) row -> row.getLong(2), Encoders.LONG())
                .mapGroupsWithState(new SSVolumeCount(config), Encoders.kryo(MutableLong.class), Encoders.kryo(Row.class), GroupStateTimeout.EventTimeTimeout());


        Dataset<Row> statusCount = records
                .repartition(statusCountThreads)
                .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(4), Encoders.INT())
                .mapGroupsWithState(new SSStatusCount(config), Encoders.LONG(), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());


        Dataset<Row> geos = records
                .repartition(geographyThreads)
                .map(new SSGeography(config), Encoders.kryo(Row.class));

        Dataset<Row> geoStats = geos
                .repartition(geoStatsThreads)
                .filter(new SSFilterNull<>())
                .groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING())
                .flatMapGroupsWithState(new SSGeoStats(config), OutputMode.Update(), Encoders.kryo(CountryStats.class), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());


        StreamingQuery counts = createMultiSink(volumeCount, volumeSink, "volumeSink", 1);
        StreamingQuery status = createMultiSink(statusCount, statusSink, "statusSink", 2);
        StreamingQuery geo = createMultiSink(geoStats, geoSink, "geoSink", 3);

        counts.awaitTermination();
        status.awaitTermination();
        geo.awaitTermination();

        return createSink();
    }

    @Override
    public String getConfigPrefix() {
        return LogProcessingConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
