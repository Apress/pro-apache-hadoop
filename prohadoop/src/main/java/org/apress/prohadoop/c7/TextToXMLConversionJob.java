package org.apress.prohadoop.c7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class TextToXMLConversionJob extends Configured implements Tool {

    public static class TextToXMLConversionMapper extends
            Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                int month = Integer.parseInt(AirlineDataUtils.getMonth(value
                        .toString().split(",")));
                context.write(key, value);
            }
        }
    }

  

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(TextToXMLConversionJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(XMLOutputFormat.class);


        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

       
        job.setMapperClass(TextToXMLConversionMapper.class);       
        job.setNumReduceTasks(0);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new TextToXMLConversionJob(), args);
    }

}
