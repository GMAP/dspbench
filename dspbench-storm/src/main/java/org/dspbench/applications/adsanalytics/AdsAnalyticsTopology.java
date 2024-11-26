package org.dspbench.applications.adsanalytics;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants;
import org.dspbench.sink.BaseSink;
import org.dspbench.spout.AbstractSpout;
import org.dspbench.topology.AbstractTopology;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdsAnalyticsTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AdsAnalyticsTopology.class);

    private AbstractSpout clickSpout;
    private AbstractSpout impressionsSpout;
    private BaseSink sink;

    private int clickSpoutThreads;
    private int impressionsSpoutThreads;
    private int sinkThreads;
    private int ctrThreads;
    private int ctrFrequency;

    public AdsAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        clickSpout  = loadSpout("click");
        impressionsSpout = loadSpout("impressions");
        sink  = loadSink();

        clickSpoutThreads = config.getInt(getConfigKey(ReinforcementLearnerConstants.Conf.SPOUT_THREADS, "click"), 1);
        impressionsSpoutThreads = config.getInt(getConfigKey(ReinforcementLearnerConstants.Conf.SPOUT_THREADS, "impressions"), 1);
        sinkThreads = config.getInt(getConfigKey(ReinforcementLearnerConstants.Conf.SINK_THREADS), 1);

        ctrThreads   = config.getInt(AdsAnalyticsConstants.Conf.CTR_THREADS, 1);
        ctrFrequency = config.getInt(AdsAnalyticsConstants.Conf.CTR_EMIT_FREQUENCY, 60);
    }

    @Override
    public StormTopology buildTopology() {
        Fields spoutFields = new Fields(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID, AdsAnalyticsConstants.Field.EVENT);
        clickSpout.setFields(spoutFields);
        impressionsSpout.setFields(spoutFields);
        
        builder.setSpout(AdsAnalyticsConstants.Component.CLICK_SPOUT, clickSpout, clickSpoutThreads);
        builder.setSpout(AdsAnalyticsConstants.Component.IMPRESSIONS_SPOUT, impressionsSpout, impressionsSpoutThreads);

        builder.setBolt(AdsAnalyticsConstants.Component.CTR, new RollingCtrBolt(ctrFrequency), ctrThreads)
               .fieldsGrouping(AdsAnalyticsConstants.Component.CLICK_SPOUT, new Fields(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID))
               .fieldsGrouping(AdsAnalyticsConstants.Component.IMPRESSIONS_SPOUT, new Fields(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID));

        builder.setBolt(AdsAnalyticsConstants.Component.SINK, sink, sinkThreads)
               .shuffleGrouping(AdsAnalyticsConstants.Component.CTR);

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return AdsAnalyticsConstants.PREFIX;
    }
    
}
