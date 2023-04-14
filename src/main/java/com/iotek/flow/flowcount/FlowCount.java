package com.iotek.flow.flowcount;


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
 * Created by Administrator on 2018/7/6.
 */
public class FlowCount {



    public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
        private Text tPhone = new Text();
        @Override
        protected void map(LongWritable key, Text sentence, Context context) throws IOException, InterruptedException {

            FlowBean flowBean = new FlowBean();

            String contents = sentence.toString();
            String[] words = contents.split("\\t");
            flowBean.setPhone(words[1]);//手机号
            flowBean.setUpFlow(Long.parseLong(words[7]));//上行流量
            flowBean.setDownFlow(Long.parseLong(words[8]));//下行流量
            //flowBean.setCountFlow(flowBean.getUpFlow() + flowBean.getDownFlow());

            tPhone.set(words[1]);

            context.write(tPhone, flowBean);
        }
    }

    public static class FlowCountReducer extends Reducer<Text, FlowBean, NullWritable, FlowBean>{
        //总体的流量bean
        private FlowBean flowBean_total = new FlowBean();
        private long upFlow_total, downFlow_total;

        private NullWritable nul = NullWritable.get();
        @Override
        protected void reduce(Text phone, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            //每一次进来统计先清零
            upFlow_total = 0;
            downFlow_total = 0;

            //然后对同一个手机号的流量进行累加，得到该手机号的总流量
            Iterator<FlowBean> iters =  values.iterator();
            while (iters.hasNext()){
                FlowBean bean = iters.next();
                upFlow_total += bean.getUpFlow();
                downFlow_total += bean.getDownFlow();
            }

            //赋值给总体flowbean
            flowBean_total.setUpFlow(upFlow_total);
            flowBean_total.setDownFlow(downFlow_total);
            flowBean_total.setCountFlow(upFlow_total + downFlow_total);
            flowBean_total.setPhone(phone.toString());

            context.write(nul, flowBean_total);

        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(FlowCount.class);
        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        //指出map阶段要处理数据的路径
        FileInputFormat.setInputPaths(job,new Path("C:\\input\\flow.log"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job,new Path("C:\\output"));
;
//        FileInputFormat.setInputPaths(job,new Path("/input/flow.txt"));
//        FileOutputFormat.setOutputPath(job,new Path("/output/result"));



        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
