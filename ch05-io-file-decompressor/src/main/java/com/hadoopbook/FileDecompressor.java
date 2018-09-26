package com.hadoopbook;// cc FileDecompressor A program to decompress a compressed file using a codec inferred from the file's extension

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

// vv FileDecompressor
public class FileDecompressor {

    public static void main(String[] args) throws Exception {

        String uri;
        if (args != null && args.length > 0) {
            uri = args[0];
        } else {
            uri = "file.gz";
        }

        Configuration conf = new Configuration();
        URI toUri = FileDecompressor.class.getClassLoader().getResource(uri).toURI();
        FileSystem fs = FileSystem.get(toUri, conf);
        Path inputPath = new Path(toUri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if (codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri,
                codec.getDefaultExtension()) + ".out.txt";
       outputUri = FileDecompressor.class.getClassLoader().getResource("").getPath();


        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
// ^^ FileDecompressor
