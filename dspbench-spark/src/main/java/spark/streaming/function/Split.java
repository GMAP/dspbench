package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
public class Split extends BaseFunction implements FlatMapFunction<Row, Row> {

    public Split(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(Row s) throws Exception {
        incReceived();
        //receiveThroughput();
        String[] words = s.getString(0).split("\\W");
        List<Row> tuples = new ArrayList<>();

        for (String word : words) {
            if (!StringUtils.isBlank(word)){
                tuples.add(RowFactory.create(word));
                incEmitted();
                //emittedThroughput();
            }
        }
        return tuples.iterator();
    }
}