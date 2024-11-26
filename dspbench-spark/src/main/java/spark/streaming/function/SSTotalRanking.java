package spark.streaming.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.streaming.tools.Rankable;
import spark.streaming.tools.RankableObjectWithFields;
import spark.streaming.tools.Rankings;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

public class SSTotalRanking extends BaseFunction implements MapFunction<Row, Row>{
    private static final Logger LOG = LoggerFactory.getLogger(SSTotalRanking.class);
        
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int emitFrequencyInSeconds;
    private final int count;
    private final Rankings rankings;

    public SSTotalRanking(Configuration config) {
        this(config, DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public SSTotalRanking(Configuration config, int topN) {
        this(config, topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public SSTotalRanking(Configuration config, int topN, int emitFrequencyInSeconds) {
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
    public Row call(Row values) throws Exception {
        
        incReceived();

        Rankings rankingsToBeMerged = (Rankings) values.get(0);
        getRankings().updateWith(rankingsToBeMerged);
        getRankings().pruneZeroCounts();

        incEmitted();
        LOG.info("Rankings: " + rankings);
        return RowFactory.create(rankings.copy());
    }
}
