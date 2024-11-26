/*
 * beymani: Outlier and anamoly detection 
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

package spark.streaming.model.predictor;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.FraudDetectionConstants;
import spark.streaming.util.Configuration;
import spark.streaming.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Predictor based on markov model
 * @author pranab
 *
 */
public class MarkovModelPredictor extends ModelBasedPredictor implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MarkovModelPredictor.class);
    
    private enum DetectionAlgorithm {
        MissProbability, 
        MissRate, 
        EntropyReduction
    };
    
    private MarkovModel markovModel;
    private Map<String, List<String>> records = new HashMap<>(); 
    private boolean localPredictor;
    private int stateSeqWindowSize;
    private int stateOrdinal;
    private DetectionAlgorithm detectionAlgorithm;
    private Map<String, Pair<Double, Double>> globalParams;
    private double metricThreshold;
    private int[] maxStateProbIndex;
    private double[] entropy;


    public MarkovModelPredictor(Configuration conf) {
        String mmKey = conf.get(FraudDetectionConstants.Config.MARKOV_MODEL_KEY, null);
        String model;
        
        if (StringUtils.isBlank(mmKey)) {
            model = getClass().getClassLoader().getResource(FraudDetectionConstants.DEFAULT_MODEL).getPath();//new MarkovModelResourceSource().getModel(FraudDetectionConstants.DEFAULT_MODEL);
        } else {
            model = new MarkovModelFileSource().getModel(mmKey);
        }
        
        markovModel = new MarkovModel(model);
        localPredictor = conf.getBoolean(FraudDetectionConstants.Config.LOCAL_PREDICTOR, true);
        
        if (localPredictor) {
            stateSeqWindowSize = conf.getInt(FraudDetectionConstants.Config.STATE_SEQ_WIN_SIZE, 5);
            LOG.info("local predictor window size:" + stateSeqWindowSize );
        }  else {
            stateSeqWindowSize = 5;
            globalParams = new HashMap<>();
        }
        
        //state value ordinal within record
        stateOrdinal = conf.getInt(FraudDetectionConstants.Config.STATE_ORDINAL, 1);

        //detection algoritm
        String algorithm = conf.get(FraudDetectionConstants.Config.DETECTION_ALGO);
        LOG.info("detection algorithm:" + algorithm);
        
        if (algorithm.equals("missProbability")) {
            detectionAlgorithm = DetectionAlgorithm.MissProbability;
        } else if (algorithm.equals("missRate")) {
            detectionAlgorithm = DetectionAlgorithm.MissRate;

            //max probability state index
            maxStateProbIndex = new int[markovModel.getNumStates()];
            for (int i = 0; i < markovModel.getNumStates(); ++i) {
                int maxProbIndex = -1;
                double maxProb = -1;
                for (int j = 0; j < markovModel.getNumStates(); ++j) {
                    if (markovModel.getStateTransitionProb()[i][j] > maxProb) {
                        maxProb = markovModel.getStateTransitionProb()[i][j];
                        maxProbIndex = j;
                    }
                }
                maxStateProbIndex[i] = maxProbIndex;
            }
        } else if (algorithm.equals("entropyReduction")) {
            detectionAlgorithm = DetectionAlgorithm.EntropyReduction;

            //entropy per source state
            entropy = new double[markovModel.getNumStates()];
            for (int i = 0; i < markovModel.getNumStates(); ++i) {
                double ent = 0;
                for (int j = 0; j < markovModel.getNumStates(); ++j) {
                    ent  += -markovModel.getStateTransitionProb()[i][j] * Math.log(markovModel.getStateTransitionProb()[i][j]);
                }
                entropy[i] = ent;
            }
        } else {
            //error
            String msg = "The detection algorithm '" + algorithm + "' does not exist";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        //metric threshold
        metricThreshold = conf.getDouble(FraudDetectionConstants.Config.METRIC_THRESHOLD, 0.90);
    }

    @Override
    public Prediction execute(String entityID, List<String> recordSeq) {
        double score = 0;


        if (null == recordSeq) {
            recordSeq = new ArrayList<>();
            records.put(entityID, recordSeq);
        }

        //add and maintain size
       // recordSeq.add(record);
        if (recordSeq.size() > stateSeqWindowSize) {
            recordSeq.remove(0);
        }

        String[] stateSeq = null;
        if (localPredictor) {
            //local metric
            LOG.debug("local metric,  seq size " + recordSeq.size());
            
            if (recordSeq.size() == stateSeqWindowSize) {
                stateSeq = new String[stateSeqWindowSize]; 
                for (int i = 0; i < stateSeqWindowSize; ++i) {
                    stateSeq[i] = recordSeq.get(i).split(",")[stateOrdinal];
                }
                score = getLocalMetric(stateSeq);
            }
        } else {
            //global metric
            LOG.debug("global metric");
            
            if (recordSeq.size() >= 2) {
                stateSeq = new String[2];
                
                for (int i = stateSeqWindowSize - 2, j =0; i < stateSeqWindowSize; ++i) {
                    stateSeq[j++] = recordSeq.get(i).split(",")[stateOrdinal];
                }
                
                Pair<Double,Double> params = globalParams.get(entityID);
                
                if (null == params) {
                    params = new Pair<>(0.0, 0.0);
                    globalParams.put(entityID, params);
                }
                
                score = getGlobalMetric(stateSeq, params);
            }
        }		

        //outlier
        LOG.debug("metric  " + entityID + ":" + score);
        
        Prediction prediction = new Prediction(entityID, score, stateSeq, (score > metricThreshold));
        
        if (score > metricThreshold) {
            /*
            StringBuilder stBld = new StringBuilder(entityID);
            stBld.append(" : ");
            for (String st : stateSeq) {
                stBld.append(st).append(" ");
            }
            stBld.append(": ");
            stBld.append(score);
            jedis.lpush(outputQueue,  stBld.toString());
            */
            // TODO should return the score and state sequence
            // should say if is an outlier or not
        }
        
        return prediction;
    }


    /**
     * @param stateSeq
     * @return
     */
    private double getLocalMetric(String[] stateSeq) {
        double metric = 0;
        double[] params = new double[2];
        params[0] = params[1] = 0;
        
        if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
            missProbability(stateSeq, params);
        } else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
            missRate(stateSeq, params);
        } else {
            entropyReduction( stateSeq, params);
        }
        
        metric = params[0] / params[1];	
        return metric;
    }	


    /**
     * @param stateSeq
     * @return
     */
    private double getGlobalMetric(String[] stateSeq, Pair<Double,Double> globParams) {
        double metric = 0;
        double[] params = new double[2];
        params[0] = params[1] = 0;
        
        if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
            missProbability(stateSeq, params);
        } else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
            missRate(stateSeq, params);
        } else {
            entropyReduction( stateSeq, params);
        }

        globParams.setLeft(globParams.getLeft() + params[0]);
        globParams.setRight(globParams.getRight() + params[1]);
        metric = globParams.getLeft() / globParams.getRight();	
        return metric;
    }	

    /**
     * @param stateSeq
     * @return
     */
    private void missProbability(String[] stateSeq, double[] params) {
        int start = localPredictor? 1 :  stateSeq.length - 1;
        for (int i = start; i < stateSeq.length; ++i ){
            int prState = markovModel.getStates().indexOf(stateSeq[i -1]);
            int cuState = markovModel.getStates().indexOf(stateSeq[i ]);
            
            LOG.debug("state prob index:" + prState + " " + cuState);

            //add all probability except target state
            for (int j = 0; j < markovModel.getStates().size(); ++ j) {
                if (j != cuState)
                    params[0] += markovModel.getStateTransitionProb()[prState][j];
            }
            params[1] += 1;
        }
        
        LOG.debug("params:" + params[0] + ":" + params[1]);
    }


    /**
     * @param stateSeq
     * @return
     */
    private void missRate(String[] stateSeq, double[] params) {
        int start = localPredictor? 1 :  stateSeq.length - 1;
        for (int i = start; i < stateSeq.length; ++i ){
            int prState = markovModel.getStates().indexOf(stateSeq[i -1]);
            int cuState = markovModel.getStates().indexOf(stateSeq[i ]);
            params[0] += (cuState == maxStateProbIndex[prState]? 0 : 1);
            params[1] += 1;
        }
    }

    /**
     * @param stateSeq
     * @return
     */
    private void entropyReduction(String[] stateSeq, double[] params) {
        int start = localPredictor? 1 :  stateSeq.length - 1;
        for (int i = start; i < stateSeq.length; ++i ){
            int prState = markovModel.getStates().indexOf(stateSeq[i -1]);
            int cuState = markovModel.getStates().indexOf(stateSeq[i ]);
            params[0] += (cuState == maxStateProbIndex[prState]? 0 : 1);
            params[1] += 1;
        }
    }
}
