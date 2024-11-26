package spark.streaming.function;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.util.Configuration;

import java.time.Instant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
/**
 * @author luandopke
 */
public class SSSensorParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSSensorParser.class);
    private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
            .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
            .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
            .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
            .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();

    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    private static final int DATE_FIELD = 0;
    private static final int TIME_FIELD = 1;
    private static final int EPOCH_FIELD = 2;
    private static final int MOTEID_FIELD = 3;
    private static final int TEMP_FIELD = 4;
    private static final int HUMID_FIELD = 5;
    private static final int LIGHT_FIELD = 6;
    private static final int VOLT_FIELD = 7;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);


    private static final ImmutableMap<String, Integer> fieldList = ImmutableMap.<String, Integer>builder()
            .put("temp", TEMP_FIELD)
            .put("humid", HUMID_FIELD)
            .put("light", LIGHT_FIELD)
            .put("volt", VOLT_FIELD)
            .build();

    private final int valueFieldKey;

    public SSSensorParser(Configuration config) {
        super(config);

        String valueField = config.get(SpikeDetectionConstants.Config.PARSER_VALUE_FIELD);
        valueFieldKey = fieldList.get(valueField);
    }

    @Override
    public Row call(String value) throws Exception {
        incReceived();
        String[] fields = value.split("\\s+");

        if (fields.length != 8)
            return null;

        String dateStr = String.format("%s %s", fields[DATE_FIELD], fields[TIME_FIELD]);
        DateTime date = null;

        try {
            date = formatterMillis.parseDateTime(dateStr);
        } catch (IllegalArgumentException ex) {
            try {
                date = formatter.parseDateTime(dateStr);
            } catch (IllegalArgumentException ex2) {
                LOG.warn("Error parsing record date/time field, input record: " + value, ex2);
                return null;
            }
        }

        try {
            incEmitted();
            return RowFactory.create(Integer.parseInt(fields[MOTEID_FIELD]),
                    date.toDate(),
                    Double.parseDouble(fields[valueFieldKey]));

        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing record numeric field, input record: " + value, ex);
        }
        return RowFactory.create();
    }
}