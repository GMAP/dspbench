package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;
import spark.streaming.util.geoip.GeoIP2Location;
import spark.streaming.util.geoip.IPLocation;
import spark.streaming.util.geoip.Location;

/**
 *
 * @author mayconbordin
 */
public class GeoFinder extends BaseFunction implements PairFunction<Tuple2<Long, Tuple>, String, Tuple> {
    private transient IPLocation resolver;
    private String dbPath;

    public GeoFinder(Configuration config) {
        super(config);
        
        this.dbPath = config.get(Configuration.GEOIP2_DB);
    }

    public IPLocation getResolver() {
        if (resolver == null) {
            resolver = new GeoIP2Location(this.dbPath);
        }
        
        return resolver;
    }
    
    @Override
    public Tuple2<String, Tuple> call(Tuple2<Long, Tuple> t) throws Exception {
        //incReceived();
        String ip = t._2.getString("ip");
        
        Location location = getResolver().resolve(ip);
        
        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();
            
            if (StringUtils.isNotBlank(country)) {
                Tuple tuple = new Tuple(t._2);
                tuple.set("city", city);
                tuple.set("count", 1L);

                //incEmitted();
                return new Tuple2<>(country, tuple);
            }
        }
                
        return null;
    }

}