package com.iotek.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;


/**
 * Created by x on 2023/4/12.
 */                                            //输入键值        输出键值
                                          //偏移量     一行数据    单词    1次
public class WordMapper extends Mapper<LongWritable ,Text,    Text,LongWritable> {

    private  Text text=new Text();
    private  LongWritable lone=new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String string = value.toString();
        String[] words = string.split(" |,"); //空格分割
        for(String s:words){
         text.set(s);
         context.write(text,lone);

        }

    }
}
