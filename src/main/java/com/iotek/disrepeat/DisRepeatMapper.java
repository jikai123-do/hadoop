package com.iotek.disrepeat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by TOSHIBA on 2017/7/19.
 */                                         //输入的键值对的类型         输出的键值对的类型
                                      //        偏移量     一行文字      单词    1次
public class DisRepeatMapper extends Mapper<LongWritable, Text,        Text, LongWritable>{
    private Text tWord = new Text();
    private LongWritable lone = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text sentence, Context context) throws IOException, InterruptedException {
        String row = sentence.toString();
        String[] words = row.split(" |,");//用空格对单词做切割，得到单词数组

        for(String word:words){ //遍历
            tWord.set(word);
            context.write(tWord, lone);
        }
    }
}
