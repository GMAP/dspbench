package spark.streaming.function;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function2;
import spark.streaming.model.gis.Road;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.List;

/**
 *
 * @author mayconbordin
 */
public class SpeedCalculator extends BaseFunction implements Function2<List<Tuple>, Optional<Object>, Optional<Object>> {

    public SpeedCalculator(Configuration config) {
        super(config);
    }
    
    private void updateTuple(Tuple state, Tuple tuple) {
        int roadID = tuple.getInt("roadID");
        int speed  = tuple.getInt("speed");
        
        int averageSpeed = 0;
        int count = 0;
        
        if (state.get("road") == null) {
            Road road = new Road(roadID);
            road.addRoadSpeed(speed);
            road.setCount(1);
            road.setAverageSpeed(speed);
            
            state.set("road", road);
            averageSpeed = speed;
            count = 1;
        } else {
            Road road = (Road) state.get("road");
            
            int sum = 0;
            
            if (road.getRoadSpeedSize() < 2) {
                road.incrementCount();
                road.addRoadSpeed(speed);
                
                for (int it : road.getRoadSpeed()) {
                    sum += it;
                }
                
                averageSpeed = (int)((double)sum/(double)road.getRoadSpeedSize());
                road.setAverageSpeed(averageSpeed);
                count = road.getRoadSpeedSize();
            } else {
                double avgLast = road.getAverageSpeed();
                double temp = 0;
                
                for (int it : road.getRoadSpeed()) {
                    sum += it;
                    temp += Math.pow((it-avgLast), 2);
                }
                
                int avgCurrent = (int) ((sum + speed)/((double)road.getRoadSpeedSize() + 1));
                temp = (temp + Math.pow((speed - avgLast), 2)) / (road.getRoadSpeedSize());
                double stdDev = Math.sqrt(temp);
                
                if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);
                    road.setAverageSpeed(avgCurrent);
                    
                    averageSpeed = avgCurrent;
                    count = road.getRoadSpeedSize();
                }
            }
        }
        
        state.set("averageSpeed", averageSpeed);
        state.set("count", count);
    }

    @Override
    public Optional<Object> call(List<Tuple> v1, Optional<Object> v2) throws Exception {
        return null;
    }
}