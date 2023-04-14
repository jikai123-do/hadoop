package com.iotek.join;


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

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Join {

    public static class JoinMapper extends Mapper<LongWritable, Text,  LongWritable, OrderDetail> {
        private NullWritable nullWritable = NullWritable.get();
        private OrderDetail orderDetail = new OrderDetail();
        private LongWritable lGoodsId = new LongWritable();

        private Map<Integer, Goods> map = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("c:\\input\\goods.txt")), "utf-8"));
            String line = null;
            while ((line = br.readLine()) != null){
                String[] words = line.split(" ");
                Goods g = new Goods();
                g.setGoodsId(Integer.parseInt(words[0]));
                g.setName(words[1]);
                g.setPrice(Integer.parseInt(words[2]));
                g.setUnit(words[3]);

                map.put(g.getGoodsId(), g);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            String[] words = content.split(" ");


            orderDetail.setOrderId(Integer.parseInt(words[0]));

            int goodsId = Integer.parseInt(words[1]);
            Goods g = map.get(goodsId);

            orderDetail.setName(g.getName());
            orderDetail.setCount(Integer.parseInt(words[2]));

            orderDetail.setPrice(g.getPrice());

            orderDetail.setUnit(g.getUnit());


            int total = orderDetail.getCount()*orderDetail.getPrice();
            orderDetail.setTotalPrice(total);

            orderDetail.setGoodsId(g.getGoodsId());
            lGoodsId.set(goodsId);



            context.write(lGoodsId, orderDetail);//value部分不能为NullWritable


        }
    }


    public static class JoinReducer extends Reducer< LongWritable, OrderDetail,  NullWritable, OrderDetail> {
        private NullWritable nullWritable = NullWritable.get();

        @Override
        protected void reduce(LongWritable goodsId, Iterable<OrderDetail> values, Context context) throws IOException, InterruptedException {
            Iterator<OrderDetail>  iters = values.iterator();
            while (iters.hasNext()){
                context.write(nullWritable, iters.next());
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指出打包的类以及mapper任务的类和reducer任务的类
        job.setJarByClass(Join.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        //指出map阶段输出的键值对类型 和reducer阶段输出的键值对类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(OrderDetail.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrderDetail.class);

        //指出map阶段要处理的数据的路径--文件夹
        FileInputFormat.setInputPaths(job, new Path("c:\\input\\orders.txt"));//hdfs路径
        //指出reducer阶段处理结果的输出路径--输出结果路径必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("c:\\output5"));////hdfs路径

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}