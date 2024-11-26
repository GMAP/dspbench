package flink.application.spikedetection;

import flink.application.AbstractApplication;
import flink.constants.SpikeDetectionConstants;
import flink.parsers.SensorParser;
import flink.source.InfSourceFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SpikeDetection extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);
    private int parserThreads;
    private int movingAverageThreads;
    private int spikeDetectorThreads;

    public SpikeDetection(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInteger(SpikeDetectionConstants.Conf.PARSER_THREADS, 1);
        movingAverageThreads = config.getInteger(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads = config.getInteger(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple3<String, Date, Double>> dataParse = data.flatMap(new SensorParser(config)).setParallelism(parserThreads);

        //DataStream<Tuple3<String, Date, Double>> dataParse = env.addSource(new SDInfSource(config, getConfigPrefix())).setParallelism(parserThreads);

        // Process
        DataStream<Tuple3<String, Double, Double>> movingAvg = dataParse.filter(value -> (value.f0 != null))
                .keyBy(value -> value.f0).flatMap(new MovingAverage(config)).setParallelism(movingAverageThreads);

        DataStream<Tuple4<String, Double, Double, String>> spikeDetect = movingAvg.flatMap(new SpikeDetect(config))
                .setParallelism(spikeDetectorThreads);

        // Sink
        createSinkSD(spikeDetect);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return SpikeDetectionConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
