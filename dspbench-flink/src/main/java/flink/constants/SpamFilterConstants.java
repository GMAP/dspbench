package flink.constants;

public interface SpamFilterConstants extends BaseConstants {
    String PREFIX = "sf";
    String DEFAULT_WORDMAP = "/spamfilter/wordmap.json";

    interface Conf extends BaseConf {
        String TRAINING_SOURCE_THREADS = "sf.training.source.threads";
        String TRAINING_PARSER_THREADS = "sf.training.parser.threads";

        String ANALYSIS_SOURCE_THREADS = "sf.analysis.source.threads";
        String ANALYSIS_PARSER_THREADS = "sf.analysis.parser.threads";

        String TOKENIZER_THREADS  = "sf.tokenizer.threads";
        String WORD_PROB_THREADS  = "sf.wordprob.threads";
        String BAYES_RULE_THREADS = "sf.bayesrule.threads";
        String BAYES_RULE_SPAM_PROB = "sf.bayesrule.spam_probability";
        String WORD_PROB_WORDMAP  = "sf.wordprob.wordmap";
        String WORD_PROB_WORDMAP_USE_DEFAULT  = "sf.wordprob.wordmap.use_default";
        
        String SINK_THREADS = "wc.sink.threads";
    }
}
