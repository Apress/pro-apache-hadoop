package org.apress.prohadoop.c8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountWithCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        String firstLt = key.toString().substring(0,1).toLowerCase();
        context.getCounter("FIRST_LETTER", firstLt).increment(sum);
        context.write(key, new IntWritable(sum));
    }
}