package com.iotek.flow.flowregion;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.util.HashMap;

/**
 * Created by Administrator on 2018/7/13.
 */
public class RegionHashParitioner extends HashPartitioner<Text, Text> {
    private static HashMap<String, Integer> map = new HashMap<String, Integer>();

    static{
        map.put("135", 0);
        map.put("136", 1);
        map.put("137", 2);
        map.put("138", 3);
        map.put("139", 4);
    }

    //该方法是在map阶段输出键值对，决定该键值对放入那个分区的时候被调用
    //所以这个地方的参数key value就是mapper的输出键值对类型
    @Override
    public int getPartition(Text phone, Text value, int numReduceTasks) {
        String pre3 = phone.toString().substring(0, 3);
        Integer regionIndex = map.get(pre3);
        if(null == regionIndex){
            return 5;
        }
        return regionIndex;
    }


}
