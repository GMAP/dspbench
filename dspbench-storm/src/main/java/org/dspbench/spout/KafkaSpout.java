package org.dspbench.spout;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Iface;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.thrift.TException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.dspbench.constants.BaseConstants.BaseConf;
import org.dspbench.spout.parser.Parser;
import org.dspbench.util.config.ClassLoaderUtils;
import org.dspbench.util.config.Configuration;
import org.dspbench.util.stream.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * This is an encapsulation of the storm-kafka-0.8-plus spout implementation.
 * It only emits tuples to the default stream, also only the first value of the parser
 * is going to be emitted, this is a limitation of the implementation, most applications
 * won't be affected.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class KafkaSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    private static org.apache.storm.kafka.spout.KafkaSpout spout;

    @Override
    protected void initialize() {
        String parserClass = config.getString(getConfigKey(BaseConf.SPOUT_PARSER, true));
        String host        = config.getString(getConfigKey(BaseConf.KAFKA_HOST, true));
        String topic       = config.getString(getConfigKey(BaseConf.KAFKA_SPOUT_TOPIC, true));
        String consumerId  = UUID.randomUUID().toString();//config.getString(getConfigKey(BaseConf.KAFKA_CONSUMER_ID));
        //String path        = config.getString(getConfigKey(BaseConf.KAFKA_ZOOKEEPER_PATH));

        Parser parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);

        Fields defaultFields = fields.get(Utils.DEFAULT_STREAM_ID);
        if (defaultFields == null) {
            throw new RuntimeException("A KafkaSpout must have a default stream");
        }

        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(host, topic)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerId)
            .setRecordTranslator(new ParserTranslator(parser, defaultFields))
            .build();

        spout = new org.apache.storm.kafka.spout.KafkaSpout<String, String>(spoutConfig);

        spout.open(config, context, collector);
    }

    @Override
    public void nextTuple() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            emittedThroughput();
        }
    
        spout.nextTuple();
    }

    @Override
    public void fail(Object msgId) {
        spout.fail(msgId);
    }

    @Override
    public void ack(Object msgId) {
        spout.ack(msgId);
    }

    @Override
    public void deactivate() {
        spout.deactivate();
    }

    @Override
    public void close() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
        spout.close();
    }

    private class ParserTranslator implements RecordTranslator<String, String> {
        private static final long serialVersionUID = -5782462870112305750L;

        private final Parser parser;
        private final Fields fields;

        public ParserTranslator(Parser parser, Fields fields) {
            this.parser = parser;
            this.fields = fields;
        }

        public List<Object> apply(ConsumerRecord<String, String> record) {
            List<StreamValues> tuples = parser.parse(record.value());

            if (tuples != null && tuples.size() > 0) {
                return tuples.get(0);
            }

            return null;
        }

        public Fields getFieldsFor(String stream) {
            return fields;
        }
    }
}