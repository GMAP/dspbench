package org.dspbench.applications.spamfilter;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.spout.AbstractSpout;
import org.dspbench.topology.AbstractTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.sink.BaseSink;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SpamFilterTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpamFilterTopology.class);
    
    private AbstractSpout trainingSpout;
    private AbstractSpout analysisSpout;
    private BaseSink sink;
    
    private int trainingSpoutThreads;
    private int analysisSpoutThreads;
    private int tokenizerThreads;
    private int wordProbThreads;
    private int bayesRuleThreads;
    private int sinkThreads;
    
    public SpamFilterTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        trainingSpout = loadSpout("training");
        analysisSpout = loadSpout("analysis");
        sink = loadSink();
        
        trainingSpoutThreads = config.getInt(getConfigKey(SpamFilterConstants.Conf.SPOUT_THREADS, "training"), 1);
        analysisSpoutThreads = config.getInt(getConfigKey(SpamFilterConstants.Conf.SPOUT_THREADS, "analysis"), 1);
        tokenizerThreads     = config.getInt(SpamFilterConstants.Conf.TOKENIZER_THREADS, 1);
        wordProbThreads      = config.getInt(SpamFilterConstants.Conf.WORD_PROB_THREADS, 1);
        bayesRuleThreads     = config.getInt(SpamFilterConstants.Conf.BAYES_RULE_THREADS, 1);
        sinkThreads          = config.getInt(getConfigKey(SpamFilterConstants.Conf.SINK_THREADS), 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        trainingSpout.setFields(new Fields(SpamFilterConstants.Field.ID, SpamFilterConstants.Field.MESSAGE, SpamFilterConstants.Field.IS_SPAM));
        analysisSpout.setFields(new Fields(SpamFilterConstants.Field.ID, SpamFilterConstants.Field.MESSAGE, SpamFilterConstants.Field.IS_SPAM));
        
        builder.setSpout(SpamFilterConstants.Component.TRAINING_SPOUT, trainingSpout, trainingSpoutThreads);
        builder.setSpout(SpamFilterConstants.Component.ANALYSIS_SPOUT, analysisSpout, analysisSpoutThreads);

        builder.setBolt(SpamFilterConstants.Component.TOKENIZER, new TokenizerBolt(), tokenizerThreads)
               .shuffleGrouping(SpamFilterConstants.Component.TRAINING_SPOUT)
               .shuffleGrouping(SpamFilterConstants.Component.ANALYSIS_SPOUT);
        
        builder.setBolt(SpamFilterConstants.Component.WORD_PROBABILITY, new WordProbabilityBolt(), wordProbThreads)
               .fieldsGrouping(SpamFilterConstants.Component.TOKENIZER, SpamFilterConstants.Stream.TRAINING, new Fields(SpamFilterConstants.Field.WORD))
               .fieldsGrouping(SpamFilterConstants.Component.TOKENIZER, SpamFilterConstants.Stream.ANALYSIS, new Fields(SpamFilterConstants.Field.WORD))
               .allGrouping(SpamFilterConstants.Component.TOKENIZER, SpamFilterConstants.Stream.TRAINING_SUM);
        
        builder.setBolt(SpamFilterConstants.Component.BAYES_RULE, new BayesRuleBolt(), bayesRuleThreads)
               .fieldsGrouping(SpamFilterConstants.Component.WORD_PROBABILITY, new Fields(SpamFilterConstants.Field.ID));
        
        builder.setBolt(SpamFilterConstants.Component.SINK, sink, sinkThreads)
               .shuffleGrouping(SpamFilterConstants.Component.BAYES_RULE);
        
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return SpamFilterConstants.PREFIX;
    }
}
