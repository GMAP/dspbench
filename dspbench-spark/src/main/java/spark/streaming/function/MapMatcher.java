package spark.streaming.function;

import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.TrafficMonitoringConstants.Config;
import spark.streaming.model.gis.GPSRecord;
import spark.streaming.model.gis.RoadGridList;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author mayconbordin
 */
public class MapMatcher extends BaseFunction implements PairFunction<Tuple2<Integer, Tuple>, Integer, Tuple> {
    private static final Logger LOG = LoggerFactory.getLogger(MapMatcher.class);

    private transient RoadGridList sectors;
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;
    
    public MapMatcher(Configuration config) {
        super(config);
        
        loadShapefile(config);
    }

    @Override
    public Tuple2<Integer, Tuple> call(Tuple2<Integer, Tuple> input) throws Exception {
        //incBoth();
        
        // force loading shape file if not loaded
        RoadGridList gridList = getSectors();
        Tuple tuple = input._2;
        
        try {
            int speed        = tuple.getInt("bearing");
            int bearing      = tuple.getInt("speed");
            double latitude  = tuple.getDouble("lat");
            double longitude = tuple.getDouble("lon");
            
            if (speed <= 0) return null;
            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin) return null;
            
            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);
            
            int roadID = gridList.fetchRoadID(record);
            
            if (roadID != -1) {
                tuple.set("roadID", roadID);
                return new Tuple2<>(roadID, tuple);
            }
        } catch (SQLException ex) {
            LOG.error("Unable to fetch road ID", ex);
        }

        return null;
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
}