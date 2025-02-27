package org.dspbench.applications.voipstream;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.Field;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.Stream;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class RCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RCRBolt.class);

    public RCRBolt() {
        super("rcr", Field.RATE);
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }
    
    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        
        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis()/1000;
            
            if (input.getSourceStreamId().equals(Stream.DEFAULT)) {
                String callee = cdr.getCalledNumber();
                filter.add(callee, 1, timestamp);
            }

            else if (input.getSourceStreamId().equals(Stream.BACKUP)) {
                String caller = cdr.getCallingNumber();
                double rcr = filter.estimateCount(caller, timestamp);
                if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                    emittedThroughput();
                }
                collector.emit(new Values(caller, timestamp, rcr, cdr));
            }
        }
    }
}
