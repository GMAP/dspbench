package spark.streaming.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.streaming.util.Configuration;

public class SSTest extends BaseFunction implements FlatMapFunction<Row, Row>{

    private static final Logger LOG = LoggerFactory.getLogger(SSTest.class);

    public SSTest(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(Row t) throws Exception {
        LOG.info("TESTE" + t.toString());
        List<Row> tuples = new ArrayList<>();

        tuples.add(RowFactory.create(t));

        return tuples.iterator();
    }
    
}
