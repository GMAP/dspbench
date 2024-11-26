package spark.streaming.function;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import scala.Tuple2;
import spark.streaming.model.gis.Road;
import spark.streaming.util.Configuration;

import java.util.Date;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
/**
 * @author luandopke
 */
public class SSSpeedCalculator extends BaseFunction implements MapGroupsWithStateFunction<Integer, Row, Road, Row> {

    public SSSpeedCalculator(Configuration config) {
        super(config);
    }

    @Override
    public Row call(Integer key, Iterator<Row> values, GroupState<Road> state) throws Exception {
        incReceived();
        if (key == 0) return null;

        int roadID = key;
        int averageSpeed = 0;
        int count = 0;
        long inittime = 0;
        while (values.hasNext()) {
            Row tuple = values.next();
            inittime = tuple.getLong(tuple.size() - 1);

            int speed = tuple.getAs("speed");
            if (!state.exists()) {
                Road road = new Road(roadID);
                road.addRoadSpeed(speed);
                road.setCount(1);
                road.setAverageSpeed(speed);

                state.update(road);
                averageSpeed = speed;
                count = 1;
            } else {
                Road road = state.get();

                int sum = 0;

                if (road.getRoadSpeedSize() < 2) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);

                    for (int it : road.getRoadSpeed()) {
                        sum += it;
                    }

                    averageSpeed = (int) ((double) sum / (double) road.getRoadSpeedSize());
                    road.setAverageSpeed(averageSpeed);
                    count = road.getRoadSpeedSize();
                } else {
                    double avgLast = road.getAverageSpeed();
                    double temp = 0;

                    for (int it : road.getRoadSpeed()) {
                        sum += it;
                        temp += Math.pow((it - avgLast), 2);
                    }

                    int avgCurrent = (int) ((sum + speed) / ((double) road.getRoadSpeedSize() + 1));
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
                state.update(road);
            }
        }
        incEmitted();
        return RowFactory.create(new Date(), roadID, averageSpeed, count, inittime);
    }
}