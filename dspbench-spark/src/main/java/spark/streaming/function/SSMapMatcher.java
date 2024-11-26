package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.TrafficMonitoringConstants.Config;
import spark.streaming.model.gis.GPSRecord;
import spark.streaming.model.gis.RoadGridList;
import spark.streaming.util.Configuration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
/**
 *
 * @author luandopke
 */
public class SSMapMatcher extends BaseFunction implements MapFunction<Row, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(SSMapMatcher.class);

    private transient RoadGridList sectors;
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);


    public SSMapMatcher(Configuration config) {
        super(config);
        
        loadShapefile(config);
    }

    private RoadGridList getSectors() {
        if (sectors == null) {
            loadShapefile(getConfiguration());
        }
        
        return sectors;
    }
    
    private void loadShapefile(Configuration config) {
        String shapeFile = config.get(Config.MAP_MATCHER_SHAPEFILE);
        
        latMin = config.getDouble(Config.MAP_MATCHER_LAT_MIN, 0);
        latMax = config.getDouble(Config.MAP_MATCHER_LAT_MAX, 0);
        lonMin = config.getDouble(Config.MAP_MATCHER_LON_MIN, 0);
        lonMax = config.getDouble(Config.MAP_MATCHER_LON_MAX, 0);
        
        if (latMin == 0 && latMax == 0 && lonMin == 0 && lonMax == 0) {
            throw new RuntimeException("Error while reading configuration file for shape file.");
        }
        
        try {
            sectors = new RoadGridList(config, shapeFile);
        } catch (SQLException | IOException ex) {
            LOG.error("Error while loading shape file", ex);
            throw new RuntimeException("Error while loading shape file");
        }
    }

    @Override
    public Integer call(Row input) throws Exception {
        incReceived();
        RoadGridList gridList = getSectors();

        try {
            int speed        = input.getInt(5);
            int bearing      = input.getInt(6);
            double latitude  = input.getDouble(3);
            double longitude = input.getDouble(4);

            if (speed <= 0) return 0;
            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin) return 0;

            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);

            int roadID = gridList.fetchRoadID(record);

            if (roadID != -1) {
                incEmitted();
                return roadID;
            }
        } catch (SQLException ex) {
            LOG.error("Unable to fetch road ID", ex);
        }

        return 0;
    }
}