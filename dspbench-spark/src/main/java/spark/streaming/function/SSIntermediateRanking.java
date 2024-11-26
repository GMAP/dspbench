package spark.streaming.function;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.streaming.tools.Rankable;
import spark.streaming.tools.RankableObjectWithFields;
import spark.streaming.tools.Rankings;
import spark.streaming.util.Configuration;

public class SSIntermediateRanking extends BaseFunction implements MapGroupsFunction<String, Row, Row>{
    private static final Logger LOG = LoggerFactory.getLogger(SSIntermediateRanking.class);
        
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int emitFrequencyInSeconds;
    private final int count;
    private final Rankings rankings;

    public SSIntermediateRanking(Configuration config) {
        this(config, DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public SSIntermediateRanking(Configuration config, int topN) {
        this(config, topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public SSIntermediateRanking(Configuration config, int topN, int emitFrequencyInSeconds) {
        super(config);

        if (topN < 1) {
          throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }
        if (emitFrequencyInSeconds < 1) {
          throw new IllegalArgumentException(
              "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }
        count = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(count);
    }

    protected Rankings getRankings() {
        return rankings;
    }

    @Override
    public Row call(String key, Iterator<Row> values) throws Exception {
        Row tuple;
        while (values.hasNext()) {
            tuple = values.next();

            incReceived();

            Rankable rankable = RankableObjectWithFields.from(tuple);
            getRankings().updateWith(rankable);
        }

        incEmitted();
        LOG.info("Rankings: " + rankings);
        return RowFactory.create(rankings.copy(), new Timestamp(Instant.now().toEpochMilli()));
    }
}
