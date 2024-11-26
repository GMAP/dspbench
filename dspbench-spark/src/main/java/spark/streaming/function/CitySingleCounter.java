package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class CitySingleCounter extends BaseFunction implements PairFunction<Tuple2<String, Tuple>, String, Tuple> {

    public CitySingleCounter(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<String, Tuple> call(Tuple2<String, Tuple> t) throws Exception {
        //incReceived();
        
        String city = t._2.getString("city");
        
        if (StringUtils.isNotBlank(city)) {
            Tuple tuple = new Tuple(t._2);
            tuple.set("count", 1L);

            return new Tuple2<>(city, tuple);
        }
                
        return null;
    }

}