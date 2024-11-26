package spark.streaming.function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class KafkaParser extends BaseFunction implements Function<ConsumerRecord<String, String>, Tuple2<String, Tuple>> {

    public KafkaParser(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<String, Tuple> call(ConsumerRecord<String, String> t1) throws Exception {
        //incEmitted();
        return new Tuple2<>(t1.value(), new Tuple());
    }
    
}
