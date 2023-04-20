package com.iotek.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HBaseMapReduceDemo {
    //create 'results' 'cf'      ---先建表，否则会报错
   //统计表中列族为baseinfo的数据中name的值出现次数
    //统计结果：
//    apple6                                      column=cf:count, timestamp=1681975386571, value=1
//    banana                                      column=cf:count, timestamp=1681975386571, value=1
    public static class MyMapper extends TableMapper<Text, IntWritable> {

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        //从hbase中读取一行数据
        //row行键     value：本行内容
        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
            String val = new String(value.getValue(Bytes.toBytes("baseinfo"), Bytes.toBytes("name")));
            text.set(val);
            context.write(text, ONE);
        }
    }

    public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(i+""));
            context.write(null, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");

        String sourceTable = "goods";
        String targetTable = "results";

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "haitong-1:2181,haitong-2:2181,haitong-3:2181");
        Job job = Job.getInstance(config);

        job.setJarByClass(HBaseMapReduceDemo.class);

        Scan scan = new Scan();
        scan.setCaching(500); // 在MR作业中，适当设置该值可提升性能
        scan.setCacheBlocks(false); // 在MR作业中，应总为false

        TableMapReduceUtil.initTableMapperJob(
                sourceTable, // 输入表
                scan, // 扫描表配置
                MyMapper.class, // mapper类
                Text.class, // mapper输出Key
                IntWritable.class, // mapper输出Value
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                targetTable, // 输出表
                MyTableReducer.class, // reducer类
                job
        );

        //job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
