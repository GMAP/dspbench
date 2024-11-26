package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.util.Configuration;
/**
 * @author luandopke
 */
public class SSWordcountParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSWordcountParser.class);

    public SSWordcountParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String input) throws Exception {
        incReceived();
        //receiveThroughput();
        if (StringUtils.isBlank(input))
            return null;

        incEmitted();
        //emittedThroughput();
        return RowFactory.create(input);
    }
}