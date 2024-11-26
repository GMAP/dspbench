package flink.application.trafficmonitoring;

import flink.application.trafficmonitoring.gis.GPSRecord;
import flink.application.trafficmonitoring.gis.RoadGridList;
import flink.constants.TrafficMonitoringConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class MapMatching extends RichFlatMapFunction<Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>, Tuple8<String, DateTime, Boolean, Integer, Integer, Double, Double, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(MapMatching.class);

    private transient RoadGridList sectors;
    private final double latMin;
    private final double latMax;
    private final double lonMin;
    private final double lonMax;
    private final String shapeFile;

    private boolean firstExec = true;

    Configuration config;

    Metrics metrics = new Metrics();

    public MapMatching(Configuration config) {
        this.config = config;
        metrics.initialize(config, this.getClass().getSimpleName());

        this.shapeFile = config.getString(TrafficMonitoringConstants.Conf.MAP_MATCHER_SHAPEFILE,
                "/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-flink/data/beijing/roads.shp");

        this.latMin = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LAT_MIN, 39.689602);
        this.latMax = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LAT_MAX, 40.122410);
        this.lonMin = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LON_MIN, 116.105789);
        this.lonMax = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LON_MAX, 116.670021);
    }

    private void loadShapefile(Configuration config) {
        try {
            sectors = new RoadGridList(config, shapeFile);
        } catch (SQLException | IOException ex) {
            LOG.error("Error while loading shape file", ex);
            throw new RuntimeException("Error while loading shape file");
        }
    }

    private RoadGridList getSectors() {
        if (sectors == null) {
            loadShapefile(config);
        }

        return sectors;
    }

    @Override
    public void flatMap(Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double> input,
            Collector<Tuple8<String, DateTime, Boolean, Integer, Integer, Double, Double, Integer>> out) {
        
        metrics.initialize(config, this.getClass().getSimpleName());
        
        if(firstExec){
            loadShapefile(config);
            firstExec = false;
        }
        //RoadGridList gridList = getSectors();
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        try {
            int speed = input.getField(3);
            int bearing = input.getField(4);
            double latitude = input.getField(5);
            double longitude = input.getField(6);

            if (speed <= 0)
                return;
            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin)
                return;

            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);

            int roadID = sectors.fetchRoadID(record);

            if (roadID != -1) {
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                out.collect(new Tuple8<String, DateTime, Boolean, Integer, Integer, Double, Double, Integer>(
                        input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, input.f6, roadID));
            }

        } catch (SQLException ex) {
            System.out.println("Unable to fetch road ID " + ex);
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
