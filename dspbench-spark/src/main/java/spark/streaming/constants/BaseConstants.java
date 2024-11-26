package spark.streaming.constants;

import org.apache.spark.SparkConf;

/**
 *
 * @author mayconbordin
 */
public interface BaseConstants {
    String BASE_PREFIX = "spark";
    
    interface BaseConfig {
        String OUTPUT_MODE         = "spark.output.mode";
        String BATCH_SIZE         = "%s.batch.size";
        String CHECKPOINT_PATH    = "%s.checkpoint.path";
        
        String SOURCE_THREADS     = "%s.source.threads";
        String SOURCE_CLASS       = "%s.source.class";
        String SOURCE_PATH        = "%s.source.path";
        String SOURCE_GENERATOR   = "%s.source.generator";
        String SOURCE_SOCKET_PORT = "%s.source.socket.port";
        String SOURCE_SOCKET_HOST = "%s.source.socket.host";
        
        String KAFKA_HOST           = "%s.kafka.zookeeper.host";
        String KAFKA_SOURCE_TOPIC   = "%s.kafka.source.topic";
        String KAFKA_ZOOKEEPER_PATH = "%s.kafka.zookeeper.path";
        String KAFKA_CONSUMER_ID    = "%s.kafka.consumer.id";
        
        String SINK_THREADS        = "%s.sink.threads";
        String PARSER_THREADS      = "%s.parser.threads";
        String SINK_CLASS          = "%s.sink.class";
        String SINK_PATH           = "%s.sink.path";
        String SINK_FORMATTER      = "%s.sink.formatter";
        String SINK_SOCKET_PORT    = "%s.sink.socket.port";
        String SINK_SOCKET_CHARSET = "%s.sink.socket.charset";

        String GEOIP_INSTANCE = "spark.geoip.instance";
        String GEOIP2_DB = "spark.geoip2.db";
        
        String DEBUG_ON = "debug.on";
    }
    
    interface MetricReporter {
        String CONSOLE = "console";
        String SLF4J   = "slf4j";
        String CSV     = "csv";
        String STATSD  = "statsd";
    }
}
