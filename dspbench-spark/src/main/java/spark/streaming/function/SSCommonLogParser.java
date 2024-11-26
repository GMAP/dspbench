package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.DateUtils;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSCommonLogParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSCommonLogParser.class);

    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
    private static final Pattern logEntryPattern = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)");

    public static final String IP        = "ip";
    public static final String TIMESTAMP = "timestamp";
    public static final String REQUEST   = "request";
    public static final String RESPONSE  = "response";
    public static final String BYTE_SIZE = "byte_size";
    private static final int NUM_FIELDS = 8;
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);

    public SSCommonLogParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String value) throws Exception {
        incReceived();
        Map<String, Object> entry = parseLine(value);

        if (entry == null) {
            LOG.warn("Unable to parse log: {}", value);
            return null;
        }

        long minute = DateUtils.getMinuteForTime((Date) entry.get(TIMESTAMP));
        incEmitted();
        return RowFactory.create(entry.get(IP), entry.get(TIMESTAMP), minute, entry.get(REQUEST), entry.get(RESPONSE), entry.get(BYTE_SIZE), Instant.now().toEpochMilli());
    }
    
    public static Map<String, Object> parseLine(String logLine) {
        Map<String, Object> entry = new HashMap<>();

        Matcher matcher = logEntryPattern.matcher(logLine);

        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            return null;
        }

        entry.put(IP, matcher.group(1));
        entry.put(TIMESTAMP, new Timestamp(dtFormatter.parseDateTime(matcher.group(4)).toDate().getTime()));
        entry.put(REQUEST, matcher.group(5));
        entry.put(RESPONSE, Integer.parseInt(matcher.group(6)));

        if (matcher.group(7).equals("-"))
            entry.put(BYTE_SIZE, 0);
        else
            entry.put(BYTE_SIZE, Integer.parseInt(matcher.group(7)));

        return entry;
    }
}