package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class BeijingTaxiTraceParser extends BaseFunction implements Function<Tuple2<String, Tuple>, Tuple2<Integer, Tuple>> {
    private static final Logger LOG = LoggerFactory.getLogger(BeijingTaxiTraceParser.class);
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    
    private static final int ID_FIELD    = 0;
    private static final int NID_FIELD   = 1;
    private static final int DATE_FIELD  = 2;
    private static final int LAT_FIELD   = 3;
    private static final int LON_FIELD   = 4;
    private static final int SPEED_FIELD = 5;
    private static final int DIR_FIELD   = 6;

    public BeijingTaxiTraceParser(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<Integer, Tuple> call(Tuple2<String, Tuple> t) throws Exception {
        //incReceived();
        
        String[] fields = t._1.replace("\"", "").split(",");
        Tuple tuple = t._2;
        
        if (fields.length != 7)
            return null;
        
        try {
            String carId  = fields[ID_FIELD];
            DateTime date = formatter.parseDateTime(fields[DATE_FIELD]);
            
            tuple.set("carId", carId);
            tuple.set("date", date);
            tuple.set("occ", true);
            tuple.set("lat", Double.parseDouble(fields[LAT_FIELD]));
            tuple.set("lon", Double.parseDouble(fields[LON_FIELD]));
            tuple.set("speed", ((Double)Double.parseDouble(fields[SPEED_FIELD])).intValue());
            tuple.set("bearing", Integer.parseInt(fields[DIR_FIELD]));
            
            int msgId = String.format("%s:%s", carId, date.toString()).hashCode();
            
            //incEmitted();
            return new Tuple2<>(msgId, tuple);
        } catch (NumberFormatException ex) {
            LOG.error("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.error("Error parsing date/time value", ex);
        }
        
        return null;
    }

}