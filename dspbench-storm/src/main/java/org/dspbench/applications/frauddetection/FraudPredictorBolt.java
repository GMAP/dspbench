package org.dspbench.applications.frauddetection;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import org.dspbench.applications.wordcount.WordCountConstants;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.dspbench.applications.frauddetection.predictor.ModelBasedPredictor;
import org.dspbench.applications.frauddetection.predictor.Prediction;
import org.dspbench.applications.frauddetection.predictor.MarkovModelPredictor;

/**
 * @author maycon
 */
public class FraudPredictorBolt extends AbstractBolt {
    private ModelBasedPredictor predictor;

    @Override
    public void initialize() {
        String strategy = config.getString(FraudDetectionConstants.Conf.PREDICTOR_MODEL);

        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        String entityID = input.getString(0);
        String record = input.getString(1);
        Prediction p = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier()) {
            if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                emittedThroughput();
            }
            collector.emit(input, new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
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
        return new Fields(FraudDetectionConstants.Field.ENTITY_ID, FraudDetectionConstants.Field.SCORE, FraudDetectionConstants.Field.STATES);
    }
}