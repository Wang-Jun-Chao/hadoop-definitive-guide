package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortByTemperatureUsingHashPartitioner extends Configured
        implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(),
                args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf,
                CompressionType.BLOCK);

        JobClient.runJob(conf);
        return 0;
    }
}
