package flink.application.trendingtopics;

import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink.application.AbstractApplication;
import flink.constants.TrendingTopicsConstants;
import flink.parsers.JsonTweetParser;
import flink.tools.Rankings;

public class TrendingTopics extends AbstractApplication{
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
        parserThreads = config.getInteger(TrendingTopicsConstants.Conf.PARSER_THREADS,1);
        topicExtractorThreads = config.getInteger(TrendingTopicsConstants.Conf.TOPIC_EXTRACTOR_THREADS,1);
        counterThreads = config.getInteger(TrendingTopicsConstants.Conf.COUNTER_THREADS,1);
        interRankThreads = config.getInteger(TrendingTopicsConstants.Conf.IRANKER_THREADS,1);
        totalRankThreads = config.getInteger(TrendingTopicsConstants.Conf.TRANKER_THREADS,1);

        counterWindowLength = config.getInteger(TrendingTopicsConstants.Conf.COUNTER_WINDOW, 10);
        counterWindowFreq = config.getInteger(TrendingTopicsConstants.Conf.COUNTER_FREQ, 60);

        TOPK = config.getInteger(TrendingTopicsConstants.Conf.TOPK, 10);
        interRankFreq = config.getInteger(TrendingTopicsConstants.Conf.IRANKER_FREQ, 2);
        totalRankFreq = config.getInteger(TrendingTopicsConstants.Conf.TRANKER_FREQ, 2);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple3<String, String, Date>> parser = data.flatMap(new JsonTweetParser(config)).setParallelism(parserThreads);

        // Process
        DataStream<Tuple1<String>> topicExtractor = parser.filter(value -> value != null).flatMap(new TopicExtractor(config)).setParallelism(topicExtractorThreads);
        
        DataStream<Tuple3<Object,Long,Integer>> counter = topicExtractor.keyBy(value -> value.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(counterWindowLength)))
            .apply(new RollingCount(config, counterWindowFreq))
            .setParallelism(counterThreads);

        //Maybe Window is not the right one in this case!!!!
        DataStream<Tuple1<Rankings>> interRanker = counter.keyBy(value -> value.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(interRankFreq)))
            .apply(new IntermediateRanking(config, TOPK, interRankFreq))
            .setParallelism(interRankThreads);
         
        DataStream<Tuple1<Rankings>> totalRanker = interRanker
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(totalRankFreq)))
            .process(new TotalRankings(config, TOPK, totalRankFreq))
            .setParallelism(totalRankThreads);

        // Sink
        createSinkTT(totalRanker);

        return env;
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
