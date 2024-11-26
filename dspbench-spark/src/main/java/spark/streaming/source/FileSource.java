package spark.streaming.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 * @author mayconbordin
 */
public class FileSource extends BaseSource {
    private int sourceThreads;
    private String sourcePath;

    @Override
    public void initialize(Configuration config, SparkSession context, String prefix) {
        super.initialize(config, context, prefix);

        sourceThreads = config.getInt(String.format(BaseConfig.SOURCE_THREADS, prefix), 1);
        sourcePath = config.get(String.format(BaseConfig.SOURCE_PATH, prefix));
    }

    @Override
    public Dataset<Row> createStream() {
        //System.out.println(sourcePath);
        return session.readStream()
                    .format("text")
                    .option("maxFilesPerTrigger", 1)
                    .option("path", sourcePath)
                    .load();
    }

}
