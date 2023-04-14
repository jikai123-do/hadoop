package com.iotek.disrepeat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指出打包的类以及mapper任务的类和reducer任务的类
        job.setJarByClass(Driver.class);
        job.setMapperClass(DisRepeatMapper.class);
        job.setReducerClass(DisRepeatReducer.class);

        //指出map阶段输出的键值对类型 和reducer阶段输出的键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指出map阶段要处理的数据的路径--文件夹
        FileInputFormat.setInputPaths(job, new Path("c:\\input\\wordcount.txt"));//hdfs路径
        //指出reducer阶段处理结果的输出路径--输出结果路径必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("c:\\output2"));////hdfs路径

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}