package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;

import spark.streaming.constants.FraudDetectionConstants;
import spark.streaming.model.FraudRecord;
import spark.streaming.model.predictor.MarkovModelPredictor;
import spark.streaming.model.predictor.ModelBasedPredictor;
import spark.streaming.model.predictor.Prediction;
import spark.streaming.util.Configuration;

import java.util.*;

/**
 * @author luandopke
 */
public class SSFraudPred extends BaseFunction implements FlatMapGroupsWithStateFunction<String, Row, FraudRecord, Row> {

    public SSFraudPred(Configuration config) {
        super(config);
    }

    private MarkovModelPredictor predictor;

    private ModelBasedPredictor createPred() {
        if (predictor == null) {
            predictor = new MarkovModelPredictor(getConfiguration());
        }

        return predictor;
    }

    @Override
    public Iterator<Row> call(String entityID, Iterator<Row> values, GroupState<FraudRecord> state) throws Exception {
        incReceived();
        createPred();
        Row tuple;
        List<Row> tuples = new ArrayList<>();
        String record;
        FraudRecord arr = null;
        Prediction p;
        while (values.hasNext()) {
            //Calculate();
            tuple = values.next();
            record = tuple.getString(1);

            if (!state.exists()) {
                arr = new FraudRecord(record);
            } else {
                arr = state.get();
                arr.add(record);
            }

            p = predictor.execute(entityID, arr.getList());

            if (p.isOutlier()) {
                incEmitted();
                tuples.add(RowFactory.create(entityID, p.getScore(), StringUtils.join(p.getStates(), ","), tuple.get(tuple.size() - 1)));
            }
        }
        state.update(arr);
        return tuples.iterator();
    }
}