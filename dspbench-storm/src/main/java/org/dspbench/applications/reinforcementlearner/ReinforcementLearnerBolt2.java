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

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.*;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.Component;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.Conf;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.Field;
import org.dspbench.applications.reinforcementlearner.learner.ReinforcementLearner;
import org.dspbench.applications.reinforcementlearner.learner.ReinforcementLearnerFactory;

/**
 * Reinforcement learner bolt. Any RL algorithm can be used
 * @author pranab
 *
 */
public class ReinforcementLearnerBolt2 extends AbstractBolt {
    private ReinforcementLearner learner;

    @Override
    public void initialize() {
        String learnerType = config.getString(Conf.LEARNER_TYPE);
        String[] actions   = config.getString(Conf.LEARNER_ACTIONS).split(";");
        
        learner =  ReinforcementLearnerFactory.create(learnerType, actions, config);
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        if (input.getSourceComponent().equals(Component.EVENT_SPOUT)) {
            // select action for next round
            String[] splitted = input.getString(4).split(",");
            String eventID = splitted[0];//input.getStringByField(Field.EVENT_ID);
            int roundNum   = Integer.parseInt(splitted[1]);//input.getIntegerByField(Field.ROUND_NUM);
            
            String[] actions = learner.nextActions(roundNum);

            if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                emittedThroughput();
            }
            collector.emit(input, new Values(eventID, actions));
        }
        
        else if (input.getSourceComponent().equals(Component.REWARD_SPOUT)) {
            // reward feedback
            String[] splitted = input.getString(4).split(",");
            String action = splitted[0];//input.getStringByField(Field.ACTION_ID);
            int reward    = Integer.parseInt(splitted[1]);//input.getIntegerByField(Field.REWARD);
            
            learner.setReward(action, reward);
        }
        
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.EVENT_ID, Field.ACTIONS);
    }
}
