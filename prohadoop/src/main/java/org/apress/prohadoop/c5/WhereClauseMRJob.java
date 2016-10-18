package org.apress.prohadoop.c5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class WhereClauseMRJob extends Configured implements Tool {

    public static class WhereClauseMapper extends
            Mapper<LongWritable, Text, NullWritable, Text> {

        private int delayInMinutes = 0;

        @Override
        public void setup(Context context) {
            this.delayInMinutes = context.getConfiguration().getInt(
                    "map.where.delay", 1);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (AirlineDataUtils.isHeader(value)) {
                return;//Only process non header rows
            }
            String[] arr = AirlineDataUtils.getSelectResultsPerRow(value);
            String depDel = arr[8];
            String arrDel = arr[9];
            int iDepDel = AirlineDataUtils.parseMinutes(depDel, 0);
            int iArrDel = AirlineDataUtils.parseMinutes(arrDel, 0);
            StringBuilder out = AirlineDataUtils.mergeStringArray(arr, ",");
            if (iDepDel >= this.delayInMinutes
                    && iArrDel >= this.delayInMinutes) {
                out.append(",").append("B");
                context.write(NullWritable.get(), new Text(out.toString()));
            } else if (iDepDel >= this.delayInMinutes) {
                out.append(",").append("O");
                context.write(NullWritable.get(), new Text(out.toString()));
            } else if (iArrDel >= this.delayInMinutes) {
                out.append(",").append("D");
                context.write(NullWritable.get(), new Text(out.toString()));
            }

        }
    }

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WhereClauseMRJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WhereClauseMapper.class);
        job.setNumReduceTasks(0);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        boolean status = job.waitForCompletion(true);

        if (status) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(new WhereClauseMRJob(), args);
    }

}