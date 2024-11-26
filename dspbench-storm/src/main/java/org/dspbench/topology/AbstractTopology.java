package org.dspbench.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.dspbench.spout.AbstractSpout;
import org.dspbench.util.config.ClassLoaderUtils;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.dspbench.constants.BaseConstants.BaseConf;
import org.dspbench.sink.BaseSink;

public abstract class AbstractTopology {
    protected TopologyBuilder builder;
    protected String topologyName;
    protected Configuration config;

    public AbstractTopology(String topologyName, Config config) {
        this.topologyName = topologyName;
        this.config = Configuration.fromMap(config);
        this.builder = new TopologyBuilder();
    }

    public String getTopologyName() {
        return topologyName;
    }
    
    protected AbstractSpout loadSpout() {
        return loadSpout(BaseConf.SPOUT_CLASS, getConfigPrefix(), "");
    }
    
    protected AbstractSpout loadSpout(String name) {
        return loadSpout(BaseConf.SPOUT_CLASS, getConfigPrefix(), name);
    }
    
    protected AbstractSpout loadSpout(String configKey, String configPrefix, String name) {
        String PH = configPrefix;
        if(!name.isEmpty()){
            PH = String.format("%s.%s", configPrefix, name);
        }
        String spoutClass = config.getString(String.format(configKey, PH));
        AbstractSpout spout = (AbstractSpout) ClassLoaderUtils.newInstance(spoutClass, "spout", getLogger());
        spout.setConfigPrefix(configPrefix);
        spout.setConfigSubPrefix(name);
        
        return spout;
    }
    
    protected BaseSink loadSink() {
        return loadSink(BaseConf.SINK_CLASS, getConfigPrefix());
    }
    
    protected BaseSink loadSink(String name) {
        return loadSink(BaseConf.SINK_CLASS, String.format("%s.%s", getConfigPrefix(), name));
    }
    
    protected BaseSink loadSink(String configKey, String configPrefix) {
        String sinkClass = config.getString(String.format(configKey, configPrefix));
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.setConfigPrefix(configPrefix);
        
        return sink;
    }
    
    /**
     * Utility method to parse a configuration key with the application prefix and
     * component prefix.
     * @param key The configuration key to be parsed
     * @param name The name of the component
     * @return The formatted configuration key
     */
    protected String getConfigKey(String key, String name) {
        return String.format(key, String.format("%s.%s", getConfigPrefix(), name));
    }
    
    /**
     * Utility method to parse a configuration key with the application prefix..
     * @param key The configuration key to be parsed
     * @return 
     */
    protected String getConfigKey(String key) {
        return String.format(key, getConfigPrefix());
    }

    public abstract void initialize();
    public abstract StormTopology buildTopology();
    public abstract Logger getLogger();
    public abstract String getConfigPrefix();
}