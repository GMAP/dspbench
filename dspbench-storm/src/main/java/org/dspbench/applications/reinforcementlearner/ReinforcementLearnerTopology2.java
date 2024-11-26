/*
 * avenir: Predictive analytic based on Hadoop Map Reduce
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.dspbench.applications.reinforcementlearner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.tuple.Fields;
import org.dspbench.spout.AbstractSpout;
import org.dspbench.spout.parser.Parser;
import org.dspbench.topology.AbstractTopology;
import org.dspbench.util.config.ClassLoaderUtils;
import org.dspbench.util.stream.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.*;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.Component;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.Conf;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.Field;
import org.dspbench.constants.BaseConstants.BaseConf;
import org.dspbench.sink.BaseSink;

/**
 * Builds and submits storm topology for reinforcement learning
 * @author pranab
 *
 */
public class ReinforcementLearnerTopology2 extends AbstractTopology{
    private static final Logger LOG = LoggerFactory.getLogger(ReinforcementLearnerTopology.class);
    
    private AbstractSpout eventSpout;
    private AbstractSpout rewardSpout;
    private BaseSink actionSink;
    
    private int eventSpoutThreads;
    private int rewardSpoutThreads;
    private int learnerThreads;
    private int sinkThreads;
    
    public ReinforcementLearnerTopology2(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        eventSpout  = loadSpout("event");
        rewardSpout = loadSpout("reward");
        actionSink  = loadSink();
        
        eventSpoutThreads  = config.getInt(getConfigKey(Conf.SPOUT_THREADS, "event"), 1);
        rewardSpoutThreads = config.getInt(getConfigKey(Conf.SPOUT_THREADS, "reward"), 1);
        learnerThreads     = config.getInt(Conf.LEARNER_THREADS, 1);
        sinkThreads        = config.getInt(getConfigKey(Conf.SINK_THREADS), 1);
    }

    @Override
    public StormTopology buildTopology() {
        eventSpout.setFields(new Fields(Field.EVENT_ID, Field.ROUND_NUM));
        rewardSpout.setFields(new Fields(Field.ACTION_ID, Field.REWARD));

        Parser parser = (Parser) ClassLoaderUtils.newInstance(config.getString("rl.event.spout.parser"), "parser", LOG);
        parser.initialize(config);

        KafkaSpoutConfig spoutEvent =  KafkaSpoutConfig.builder("10.32.45.44:9092", "event")
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
            .setMaxUncommittedOffsets(1000000)
            .build();
            
        KafkaSpout kafkaEvent = new KafkaSpout(spoutEvent);

        KafkaSpoutConfig spoutReward =  KafkaSpoutConfig.builder("10.32.45.44:9092", "reward")
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
            .setMaxUncommittedOffsets(1000000)
            .build();
            
        KafkaSpout kafkaReward = new KafkaSpout(spoutReward);
        
        builder.setSpout(Component.EVENT_SPOUT, kafkaEvent, eventSpoutThreads);
        builder.setSpout(Component.REWARD_SPOUT, kafkaReward, rewardSpoutThreads);

        builder.setBolt(Component.LEARNER, new ReinforcementLearnerBolt2(), learnerThreads)
               .shuffleGrouping(Component.EVENT_SPOUT)
               .allGrouping(Component.REWARD_SPOUT);
        
        builder.setBolt(Component.SINK, actionSink, sinkThreads)
               .shuffleGrouping(Component.LEARNER);

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
