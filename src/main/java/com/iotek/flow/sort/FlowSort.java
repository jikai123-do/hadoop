package com.iotek.flow.sort;



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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2018/7/6.
 */
public class FlowSort {
    public static class FlowSortMapper extends Mapper<LongWritable, Text, MyLongWritable, FlowBean>{
        MyLongWritable countFlow = new MyLongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FlowBean flowBean = new FlowBean();

            String content = value.toString();
            String[] words = content.split(",");

            flowBean.setPhone(words[0]);
            flowBean.setUpFlow(Long.parseLong(words[1]));
            flowBean.setDownFlow(Long.parseLong(words[2]));
            flowBean.setCountFlow(Long.parseLong(words[3]));

            countFlow.set(flowBean.getCountFlow());
            context.write(countFlow, flowBean);//该阶段已经根据countFlow进行了排序
        }
    }

    public static class FlowSortReducer extends Reducer<MyLongWritable, FlowBean, NullWritable, FlowBean>{
        private NullWritable nul = NullWritable.get();
        @Override
        protected void reduce(MyLongWritable key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            Iterator<FlowBean>  iters = values.iterator();
            while (iters.hasNext()){
                FlowBean flowBean = iters.next();
                context.write(nul, flowBean);//已经按照流量从大到小排列
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(FlowSort.class);
        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(MyLongWritable.class);
        job.setMapOutputValueClass(FlowBean.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        //指出map阶段要处理数据的路径
        //该输入文件来源于flowcount执行完之后的结果，文件中包含手机号、上行流量、下行流量、总流量
        FileInputFormat.setInputPaths(job, new Path("C:\\output\\sortfile.txt"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("C:\\output4"));

        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
