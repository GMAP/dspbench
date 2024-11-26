package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.BaseConstants;
import spark.streaming.util.Configuration;
import spark.streaming.util.geoip.IPLocationFactory;
import spark.streaming.util.geoip.Location;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSGeography extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSGeography.class);

    private String ipResolver;
    //private static Map<String, Long> throughput = new HashMap<>();

  //  private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);

    public SSGeography(Configuration config) {
        super(config);
        ipResolver = config.get(BaseConstants.BaseConfig.GEOIP_INSTANCE);
    }

    @Override
    public Row call(Row input) throws Exception {
        incReceived();
        String ip = input.getString(0);

        Location location = IPLocationFactory.create(ipResolver, super.getConfiguration()).resolve(ip);

        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();
            incEmitted();
            return RowFactory.create(country, city, input.get(input.size() - 1));
        }
        return null;
    }
}