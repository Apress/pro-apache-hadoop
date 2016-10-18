package org.apress.prohadoop.c9;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class WordCountWithLogging {
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        public static Logger logger = Logger.getLogger(MyMapper.class);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String w = value.toString();
            logger.info("Mapper Key =" + key);
            if(logger.isDebugEnabled()){
                logger.info("Mapper value =" + value);
            }
            context.write(new Text(w), new IntWritable(1));
        }
    }

    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        public static Logger logger = Logger.getLogger(MyReducer.class);
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            logger.info("Reducer Key =" + key);
            if(logger.isDebugEnabled()){
                logger.info("Reducer value =" + sum);
            }
            System.out.println("Reducer system.out >>> " + key);
            System.err.println("Reducer system.err >>> " + key);
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCountWithLogging.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true);
        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}