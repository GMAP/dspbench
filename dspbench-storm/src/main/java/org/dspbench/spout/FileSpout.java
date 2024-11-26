package org.dspbench.spout;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.lang3.StringUtils;
import org.dspbench.constants.BaseConstants;
import org.dspbench.util.config.ClassLoaderUtils;
import org.dspbench.util.config.Configuration;
import org.dspbench.util.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.spout.parser.Parser;
import org.dspbench.util.stream.StreamValues;

import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Iface;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.thrift.TException;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);
    
    protected Parser parser;
    protected File[] files;
    protected Scanner scanner;
    protected int curFileIndex = 0;
    protected int curLineIndex = 0;
    private boolean finished = false;
    
    protected int taskId;
    protected int numTasks;

    @Override
    public void initialize() {
        taskId   = context.getThisTaskIndex();//context.getThisTaskId();
        numTasks = config.getInt(getConfigKey(BaseConstants.BaseConf.SPOUT_THREADS, true));
        
        String parserClass = config.getString(getConfigKey(BaseConstants.BaseConf.SPOUT_PARSER, true));
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        buildIndex();
        openNextFile();
    }
    
    protected void buildIndex() {
        String path = config.getString(getConfigKey(BaseConstants.BaseConf.SPOUT_PATH, true));
        if (StringUtils.isBlank(path)) {
            LOG.error("The source path has not been set");
            throw new RuntimeException("The source path has to beeen set");
        }
        
        LOG.info("Source path: {}", path);
        
        File dir = new File(path);
        if (!dir.exists()) {
            LOG.error("The source path {} does not exists", path);
            throw new RuntimeException("The source path '" + path + "' does not exists");
        }
        
        if (dir.isDirectory()) {
            files = dir.listFiles();
        } else {
            files = new File[1];
            files[0] = dir;
        }
        
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                int res = f1.lastModified() < f2.lastModified() ? -1 : ( f1.lastModified() > f2.lastModified() ? 1 : 0);
                return res;
            }
        });
        
        LOG.info("Number of files to read: {}", files.length);
    }

    @Override
    public void nextTuple() {
        String value = readFile();
        /*
        if (value == null){
            Map conf = Utils.readStormConfig();
            NimbusClient cc = NimbusClient.getConfiguredClient(conf);
            Iface client = cc.getClient();
            KillOptions opts = new KillOptions();
            opts.set_wait_secs(120);
            try {
                client.killTopologyWithOpts(config.getString("topologyName"), opts);
            } catch (NotAliveException e) {
                LOG.info("Tried to kill topology, but NotAlive");
                e.printStackTrace();
            } catch (AuthorizationException e) {
                LOG.info("Tried to kill topology, but AuthorizationException");
                e.printStackTrace();
            } catch (TException e) {
                LOG.info("Tried to kill topology, but TException");
                e.printStackTrace();
            }

            return;
        }
        */
        if (value != null) {
            List<StreamValues> tuples = parser.parse(value);
            long unixTime = 0;
            if (tuples != null) {
                for (StreamValues values : tuples) {
                    String msgId = String.format("%d%d", curFileIndex, curLineIndex);
                    collector.emit(values.getStreamId(), values, msgId);
                }
            }
        }
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            emittedThroughput();
        }
    }

    @Override
    public void close() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    protected String readFile() {
        if (finished) return null;
        
        String record = null;
                
        if (scanner.hasNextLine()) {
            record = readLine();
        } else {
            if ((curFileIndex+1) < files.length) {
                openNextFile();
                if (scanner.hasNextLine()) {
                     record = readLine();
                }				 
            } else {
                LOG.info("No more files to read");
                finished = true;
            }
        }
        
        return record;
    }
    
    /**
     * Read one line from the currently open file. If there's only one file, each
     * instance of the spout will read only a portion of the file.
     * @return The line
     */
    protected String readLine() {
        if (files.length == 1) {
            while (scanner.hasNextLine() && ++curLineIndex % numTasks != taskId)
                scanner.nextLine();
        }
        
        if (scanner.hasNextLine())
            return scanner.nextLine();
        else
            return null;
    }

    /**
     * Opens the next file from the index. If there's multiple instances of the
     * spout, it will read only a portion of the files.
     */
    protected void openNextFile() {
        while ((curFileIndex+1) % numTasks != taskId) {
            curFileIndex++;
        }

        if (curFileIndex < files.length) {
            try {
                File file = files[curFileIndex];
                scanner = new Scanner(file);
                curLineIndex = 0;
                
                LOG.info("Opened file {}, size {}", file.getName(), FileUtils.humanReadableByteCount(file.length()));
            } catch (FileNotFoundException ex) {
                LOG.error(String.format("File %s not found", files[curFileIndex]), ex);
                throw new IllegalStateException("File not found", ex);
            }
        }
    }
}	
