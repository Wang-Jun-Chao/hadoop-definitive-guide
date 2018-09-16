package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SmallFilesToSequenceFileConverter extends Configured
        implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setInputFormat(WholeFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(BytesWritable.class);

        conf.setMapperClass(SequenceFileMapper.class);
        conf.setReducerClass(IdentityReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    static class SequenceFileMapper extends MapReduceBase
            implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        private JobConf conf;

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
        }

        @Override
        public void map(NullWritable key, BytesWritable value,
                        OutputCollector<Text, BytesWritable> output, Reporter reporter)
                throws IOException {

            String filename = conf.get("map.input.file");
            output.collect(new Text(filename), value);
        }

    }
}
