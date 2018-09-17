package com.hadoopbook;// cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

// vv FileSystemDoubleCat
public class FileSystemDoubleCat {
    private static Logger logger = LoggerFactory.getLogger(FileSystemDoubleCat.class);
    public static void main(String[] args) throws Exception {
        args = new String[]{"hdfs://192.168.241.129:9000/tom/quangle.txt"};

        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0); // go back to the start of the file
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
// ^^ FileSystemDoubleCat
