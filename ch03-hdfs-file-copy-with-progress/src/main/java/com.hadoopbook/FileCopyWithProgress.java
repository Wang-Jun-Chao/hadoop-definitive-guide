// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress
package com.hadoopbook;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

// vv FileCopyWithProgress
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception {

        args = new String[]{
                "D:\\IdeaProject\\hadoop权威指南\\ch03-hdfs-file-copy-with-progress\\src\\main\\resources\\1400-8.txt",
                "hdfs://192.168.241.129:9000/tom/1400-8.txt"};
        String localSrc = args[0];
        String dst = args[1];


        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
// ^^ FileCopyWithProgress
