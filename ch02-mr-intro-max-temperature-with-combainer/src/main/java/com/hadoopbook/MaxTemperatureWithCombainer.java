package com.hadoopbook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MaxTemperatureWithCombainer {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: wjc.hadoop.MaxTemperatureWithCombainer <input path> <output path>");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss.SSS");
            args = new String[]{"/sample", "/output-" + sdf.format(new Date())};
        }

        //如果没有这句话，会报找不到winutils.exe的错误，也不知道是不是由于我修改了环境变量之后没有重启的原因。
        System.setProperty("hadoop.home.dir", "D:/DevelopTools/hadoop-2.8.3");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.241.129:9000");
        //意思是跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，
        // 去网上搜答案很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，
        // 但是其实添加了这行代码就可以了。
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //集群的方式运行，非本地运行。
        conf.set("mapreduce.framework.name", "yarn");
        String filename = "D:\\IdeaProject\\hadoop权威指南\\ch02-mr-intro-max-temperature-with-combainer\\target\\ch02-mr-intro-max-temperature-with-combainer-4.0-jar-with-dependencies.jar";
        conf.set("mapred.jar", filename);

        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxTemperatureWithCombainer.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
