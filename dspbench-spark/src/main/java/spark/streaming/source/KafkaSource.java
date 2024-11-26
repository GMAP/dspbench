package spark.streaming.source;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.function.KafkaParser;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class KafkaSource extends BaseSource {
    private String kafkaTopics;
    private String kafkaHost;
    private int sourceThreads;
    private int batchSize;
    @Override
    public void initialize(Configuration config, SparkSession context, String prefix) {
        super.initialize(config, context, prefix);

        kafkaHost = config.get(String.format(BaseConfig.KAFKA_HOST, prefix));
        kafkaTopics = config.get(String.format(BaseConfig.KAFKA_SOURCE_TOPIC, prefix));
        sourceThreads = config.getInt(String.format(BaseConfig.SOURCE_THREADS, prefix), 1);
        batchSize = config.getInt(String.format(BaseConfig.BATCH_SIZE, prefix), 1000);
    }

    @Override
    public Dataset<Row> createStream() {
        return session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaHost)
                .option("subscribe", kafkaTopics)
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", batchSize)
                .load()
                //.repartition(sourceThreads)
                .selectExpr("CAST(value AS STRING)");
    }
    
}
