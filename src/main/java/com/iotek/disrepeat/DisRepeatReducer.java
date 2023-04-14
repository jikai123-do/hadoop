package com.iotek.disrepeat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by TOSHIBA on 2017/7/19.
 */
                                      // map阶段输出的键值对的类型就是reduce阶段输入的键值对的类型
public class DisRepeatReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
    private NullWritable nullWritable = NullWritable.get();
    @Override
    protected void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {



        context.write(word, nullWritable);

    }
}
