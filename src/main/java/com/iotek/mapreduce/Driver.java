package com.iotek.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by x on 2023/4/12.
 */
public class Driver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
      //在windows环境下调试程序时将hadoop解压缩，指定路径即可进行本地调试
      //  System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");

        Configuration configuration=new Configuration() ;
        Job instance = Job.getInstance(configuration);
        //指出打包类及mapper、reduce类
        instance.setJarByClass(Driver.class);
        instance.setMapperClass(WordMapper.class);
        instance.setReducerClass(WordReduce.class);

        //map阶段输出类型
        instance.setMapOutputKeyClass(Text.class );
        instance.setMapOutputValueClass(LongWritable.class);
       //reduce阶段输出类型
        instance.setOutputKeyClass(Text.class );
        instance.setMapOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(instance,new Path("/input/wordcount.txt"));
        FileOutputFormat.setOutputPath(instance,new Path("/output/result"));

        //windows本地测试代码，测试无误后再上传文件执行任务
//        FileInputFormat.setInputPaths(instance,new Path("C:\\input"));
//        FileOutputFormat.setOutputPath(instance,new Path("C:\\output"));


        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);
    }




}
