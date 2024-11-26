package spark.streaming.function;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import spark.streaming.util.Configuration;

public class SSTopicExtractor extends BaseFunction implements FlatMapFunction<Row, Row>{

    public SSTopicExtractor(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(Row t) throws Exception {
        List<Row> tuples = new ArrayList<>();
        String text = (String) t.getString(1);
        incReceived();
        
        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    incEmitted();
                    //collector.emit(input, new Values(term));
                    tuples.add(RowFactory.create(term, new Timestamp(Instant.now().toEpochMilli())));
                }
            }
        }

        return tuples.iterator();
    }
    
}
