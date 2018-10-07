package com.hadoopbook;// cc PooledStreamCompressor A program to compress data read from standard input and write it to standard output using a pooled compressor

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.ByteArrayInputStream;

// vv PooledStreamCompressor
public class PooledStreamCompressor {

    public static void main(String[] args) throws Exception {
        String codecClassname;
        if (args != null && args.length > 0) {
            codecClassname = args[0];
        } else {
            codecClassname = GzipCodec.class.getName();
        }

        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(codecClass, conf);
        /*[*/
        Compressor compressor = null;
        try {
            compressor = CodecPool.getCompressor(codec);/*]*/
            CompressionOutputStream out =
                    codec.createOutputStream(System.out, /*[*/compressor/*]*/);
//            IOUtils.copyBytes(System.in, out, 4096, false);
            IOUtils.copyBytes(new ByteArrayInputStream("Hello World".getBytes()), out, 4096, true);
            out.finish();
            /*[*/
        } finally {
            CodecPool.returnCompressor(compressor);
        }/*]*/
    }
}
// ^^ PooledStreamCompressor
