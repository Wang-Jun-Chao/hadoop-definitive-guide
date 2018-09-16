package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class PartitionByStationYearUsingMultipleOutputFormat extends Configured
        implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new PartitionByStationYearUsingMultipleOutputFormat(), args);
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
        conf.setReducerClass(StationReducer.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputFormat(StationNameMultipleTextOutputFormat.class);

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

    static class StationReducer extends MapReduceBase
            implements Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<NullWritable, Text> output, Reporter reporter)
                throws IOException {
            while (values.hasNext()) {
                output.collect(NullWritable.get(), values.next());
            }
        }
    }

    static class StationNameMultipleTextOutputFormat
            extends MultipleTextOutputFormat<NullWritable, Text> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        protected String generateFileNameForKeyValue(NullWritable key, Text value,
                                                     String name) {
            parser.parse(value);
            return parser.getStationId() + "/" + parser.getYear();
        }
    }
}
