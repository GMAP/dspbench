package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.DateUtils;
import spark.streaming.util.Tuple;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author mayconbordin
 */
public class CommonLogParser extends BaseFunction implements Function<Tuple2<String, Tuple>, Tuple2<Long, Tuple>> {
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
    private static final Pattern logEntryPattern = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)");
    
    public static final String IP        = "ip";
    public static final String TIMESTAMP = "timestamp";
    public static final String TIMESTAMP_MINUTES = "timestamp_minutes";
    public static final String REQUEST   = "request";
    public static final String RESPONSE  = "response";
    public static final String BYTE_SIZE = "byte_size";
    private static final int NUM_FIELDS = 8;

    public CommonLogParser(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<Long, Tuple> call(Tuple2<String, Tuple> t) throws Exception {
        //incReceived();
        Matcher matcher = logEntryPattern.matcher(t._1);
        
        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            return null;
        }
        
        t._2.set(IP, matcher.group(1));
        t._2.set(TIMESTAMP, dtFormatter.parseDateTime(matcher.group(4)).toDate());
        t._2.set(REQUEST, matcher.group(5));
        t._2.set(RESPONSE, Integer.parseInt(matcher.group(6)));
        t._2.set(TIMESTAMP_MINUTES, DateUtils.getMinuteForTime((Date) t._2.get(TIMESTAMP)));
        
        if (matcher.group(7).equals("-"))
            t._2.set(BYTE_SIZE, 0);
        else
            t._2.set(BYTE_SIZE, Integer.parseInt(matcher.group(7)));

        long id = String.format("%s:%s", t._2.get(IP), t._2.get(TIMESTAMP)).hashCode();
        
        //incEmitted();
        return new Tuple2<>(id, t._2);
    }

}