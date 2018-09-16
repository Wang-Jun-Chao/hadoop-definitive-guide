package oldapi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class JoinRecordMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, TextPair, Text> {
    private NcdcRecordParser parser = new NcdcRecordParser();

    public void map(LongWritable key, Text value,
                    OutputCollector<TextPair, Text> output, Reporter reporter)
            throws IOException {

        parser.parse(value);
        output.collect(new TextPair(parser.getStationId(), "1"), value);
    }
}
