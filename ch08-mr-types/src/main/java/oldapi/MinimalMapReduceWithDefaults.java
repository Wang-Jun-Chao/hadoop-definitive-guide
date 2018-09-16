package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MinimalMapReduceWithDefaults extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        /*[*/
        conf.setInputFormat(TextInputFormat.class);

        conf.setNumMapTasks(1);
        conf.setMapperClass(IdentityMapper.class);
        conf.setMapRunnerClass(MapRunner.class);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setPartitionerClass(HashPartitioner.class);

        conf.setNumReduceTasks(1);
        conf.setReducerClass(IdentityReducer.class);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setOutputFormat(TextOutputFormat.class);/*]*/

        JobClient.runJob(conf);
        return 0;
    }
}
