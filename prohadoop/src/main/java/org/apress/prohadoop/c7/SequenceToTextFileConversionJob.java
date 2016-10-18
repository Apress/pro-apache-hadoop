package org.apress.prohadoop.c7;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.c6.DelaysWritable;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class SequenceToTextFileConversionJob extends Configured implements Tool {

    public static class SequenceToTextConversionMapper extends
            Mapper<LongWritable, DelaysWritable, NullWritable, Text> {

        public void map(LongWritable key, DelaysWritable value, Context context)
                throws IOException, InterruptedException {
            Text line = AirlineDataUtils.parseDelaysWritableToText(value);
            context.write(NullWritable.get(), line);

        }
    }

  

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(SequenceToTextFileConversionJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SequenceToTextConversionMapper.class);       
        job.setNumReduceTasks(0);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
       
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SequenceToTextFileConversionJob(), args);
    }

}
