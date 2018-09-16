package oldapi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class WholeFileInputFormat
        extends FileInputFormat<NullWritable, BytesWritable> {

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> getRecordReader(
            InputSplit split, JobConf job, Reporter reporter) throws IOException {

        return new WholeFileRecordReader((FileSplit) split, job);
    }
}
