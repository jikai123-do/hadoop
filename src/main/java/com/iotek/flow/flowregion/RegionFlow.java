package com.iotek.flow.flowregion;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2018/7/13.
 */
public class RegionFlow  {
    public static class RegionFlowMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text tPhone = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] words = str.split("\t");

            if(words[1].length() != 11){//获取的时第二列的手机号--应该是接电话那方
                return;
            }
            tPhone.set(words[1]);
            context.write(tPhone, value);
        }
    }

    public static class RegionFlowReducer extends Reducer< Text, Text, Text, NullWritable>{
        private NullWritable nul = NullWritable.get();
        @Override
        protected void reduce(Text phone, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iters = values.iterator();
            while (iters.hasNext()){
                context.write(iters.next(), nul);
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(RegionFlow.class);
        job.setPartitionerClass(RegionHashParitioner.class);//按手机号分区分别生成文件，总共可能有6个文件
        job.setNumReduceTasks(6);

        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(RegionFlowMapper.class);
        job.setReducerClass(RegionFlowReducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指出map阶段要处理数据的路径
        FileInputFormat.setInputPaths(job, new Path("c:\\input\\flow.log"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("c:\\output3"));

        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
