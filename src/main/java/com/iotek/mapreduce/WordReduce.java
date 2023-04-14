package com.iotek.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by x on 2023/4/12.
 */                              //mapper输出的就是reduce输入的
public class WordReduce extends Reducer<Text,LongWritable, Text,LongWritable> {


    private long count;
    private  LongWritable lone=new LongWritable();


    //reduce接到的数据类似于<k1,{<v1>,<v2>,<v3>}>这种格式
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        count=0;
        Iterator<LongWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            LongWritable longWritable = iterator.next();
           count=count+longWritable.get();
        }
        lone.set(count);


        context.write(key,lone);
    }


}
