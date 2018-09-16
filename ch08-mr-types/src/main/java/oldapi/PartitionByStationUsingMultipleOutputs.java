package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class PartitionByStationUsingMultipleOutputs extends Configured
        implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
                args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setMapperClass(StationMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setReducerClass(MultipleOutputsReducer.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputFormat(NullOutputFormat.class); // suppress empty part file

        MultipleOutputs.addMultiNamedOutput(conf, "station", TextOutputFormat.class,
                NullWritable.class, Text.class);

        JobClient.runJob(conf);
        return 0;
    }

    static class StationMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            parser.parse(value);
            output.collect(new Text(parser.getStationId()), value);
        }
    }

    static class MultipleOutputsReducer extends MapReduceBase
            implements Reducer<Text, Text, NullWritable, Text> {

        private MultipleOutputs multipleOutputs;

        @Override
        public void configure(JobConf conf) {
            multipleOutputs = new MultipleOutputs(conf);
        }

        @SuppressWarnings("unchecked")
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<NullWritable, Text> output, Reporter reporter)
                throws IOException {

            OutputCollector collector = multipleOutputs.getCollector("station",
                    key.toString().replace("-", ""), reporter);
            while (values.hasNext()) {
                collector.collect(NullWritable.get(), values.next());
            }
        }

        @Override
        public void close() throws IOException {
            multipleOutputs.close();
        }
    }
}
