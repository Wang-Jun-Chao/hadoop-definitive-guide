package com.hadoopbook;

// cc StreamCompressor A program to compress data read from standard input and write it to standard output

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.ByteArrayInputStream;

// vv StreamCompressor
public class StreamCompressor {

    public static void main(String[] args) throws Exception {

        String codecClassname;
        if (args != null && args.length > 0) {
            codecClassname = args[0];
        } else {
            codecClassname = GzipCodec.class.getName();
        }


        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        CompressionOutputStream out = codec.createOutputStream(System.out);
//        IOUtils.copyBytes(System.in, out, 4096, false);
        IOUtils.copyBytes(new ByteArrayInputStream("Hello World".getBytes()), out, 4096, true);
        out.finish();

    }
}
// ^^ StreamCompressor
