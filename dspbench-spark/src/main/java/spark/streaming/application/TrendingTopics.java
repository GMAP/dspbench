package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.streaming.constants.TrendingTopicsConstants;
import spark.streaming.function.SSFilterNull;
import spark.streaming.function.SSIntermediateRanking;
import spark.streaming.function.SSRollingCounter;
import spark.streaming.function.SSTopicExtractor;
import spark.streaming.function.SSTotalRanking;
import spark.streaming.function.SSTweetParser;
import spark.streaming.function.SSWordCount;
import spark.streaming.tools.Rankings;
import spark.streaming.util.Configuration;

public class TrendingTopics extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(TrendingTopics.class);

    private int parserThreads;
    private int topicExtractorThreads;
    private int counterThreads;
    private int interRankThreads;
    private int totalRankThreads;
    private int counterWindowLength;
    private int counterWindowFreq;
    private int TOPK;
    private int interRankFreq;
    private int totalRankFreq;

    public TrendingTopics(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(TrendingTopicsConstants.Config.PARSER_THREADS, 1);
        topicExtractorThreads = config.getInt(TrendingTopicsConstants.Config.TOPIC_EXTRACTOR_THREADS, 1);
        counterThreads = config.getInt(TrendingTopicsConstants.Config.COUNTER_THREADS, 1);
        interRankThreads = config.getInt(TrendingTopicsConstants.Config.IRANKER_THREADS, 1);
        totalRankThreads = config.getInt(TrendingTopicsConstants.Config.TRANKER_THREADS, 1);
        counterWindowLength = config.getInt(TrendingTopicsConstants.Config.COUNTER_WINDOW, 10);
        counterWindowFreq = config.getInt(TrendingTopicsConstants.Config.COUNTER_FREQ, 60);
        TOPK = config.getInt(TrendingTopicsConstants.Config.TOPK, 10);
        interRankFreq = config.getInt(TrendingTopicsConstants.Config.IRANKER_FREQ, 2);
        totalRankFreq = config.getInt(TrendingTopicsConstants.Config.TRANKER_FREQ, 2);
    }

    @Override
    public DataStreamWriter<Row> buildApplication() throws StreamingQueryException {

        StructType topicExtractST = new StructType(new StructField[]{
                new StructField("term", DataTypes.StringType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty())
        });

        StructType counterST = new StructType(new StructField[]{
            new StructField("obj", DataTypes.StringType, true, Metadata.empty()),
            new StructField("count", DataTypes.LongType, true, Metadata.empty()),
            new StructField("windowLength", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty())
        });
        
        // Source
        Dataset<Row> input = createSource();

        //Parser
        Dataset<Row> records = input.repartition(parserThreads)
            .as(Encoders.STRING())
            .map(new SSTweetParser(config), Encoders.kryo(Row.class));

        //Process
        Dataset<Row> topicExtractor = records.repartition(topicExtractorThreads)
            .filter(new SSFilterNull<>())
            .flatMap(new SSTopicExtractor(config),  Encoders.row(topicExtractST));

        Dataset<Row> counter = topicExtractor.repartition(counterThreads)
            .withWatermark("timestamp", Integer.toString(counterWindowLength)+" seconds")
            .groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING())
            .flatMapGroups(new SSRollingCounter(config, counterWindowFreq), Encoders.kryo(Row.class)); //Encoders.row(counterST)

        Dataset<Row> interRanker = counter.repartition(interRankThreads)
            //.withWatermark("timestamp", Integer.toString(interRankFreq)+" seconds")
            //.groupBy(functions.window(counter.col("timestamp"), Integer.toString(interRankFreq)+ " seconds"), counter.col("obj"))
            .groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING()) //Cannot use window together with keyedGroupBy
            .mapGroups(new SSIntermediateRanking(config, TOPK, interRankFreq), Encoders.kryo(Row.class));

        Dataset<Row> totalRanker = interRanker.repartition(totalRankThreads)
            //.withWatermark("timestamp", Integer.toString(totalRankFreq)+" seconds")
            .map(new SSTotalRanking(config, TOPK, totalRankFreq), Encoders.kryo(Row.class));

        //Sink
        return createSink(totalRanker);
    }

    @Override
    public String getConfigPrefix() {
        return TrendingTopicsConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
