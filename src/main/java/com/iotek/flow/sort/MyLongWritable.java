package com.iotek.flow.sort;

import org.apache.hadoop.io.LongWritable;

/**
 * Created by Administrator on 2018/7/6.
 */
public class MyLongWritable extends LongWritable implements Comparable<LongWritable> {
    @Override
    public int compareTo(LongWritable o) {
        long v = this.get() - o.get();
        if(v > 0){
            return -1;
        }else if(v < 0){
            return 1;
        }

        return 0;

    }
}

