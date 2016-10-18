package org.apress.prohadoop.c6;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class MultiOutputMRJob extends Configured implements Tool {

    public static class MultiOutputMapper extends
            Mapper<LongWritable, Text, NullWritable, Text> {
        enum OnTimeStatistics {
            DepOnTimeArrOnTime, DepOnTimeArrDelayed, DepDelayedArrOnTime, DepDelayedArrDelayed
        }

        private MultipleOutputs mos;

        @Override
        public void setup(Context context) {
            this.mos = new MultipleOutputs(context);

        }

        public void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(line)) {
                String[] contents = line.toString().split(",");
                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(line
                        .toString());
                int arrivalDelay = dw.arrDelay.get();
                int departureDelay = dw.depDelay.get();
                Text value = AirlineDataUtils.parseDelaysWritableToText(dw);

                if (arrivalDelay <= 0 && departureDelay <= 0) {
                    this.mos.write("OnTimeDepOnTimeArr", NullWritable.get(),
                            value);
                    context.getCounter(OnTimeStatistics.DepOnTimeArrOnTime)
                            .increment(1);
                }
                if (arrivalDelay <= 0 && departureDelay > 0) {
                    this.mos.write("DelayedDepOnTimeArr", NullWritable.get(),
                            value);
                    context.getCounter(OnTimeStatistics.DepDelayedArrOnTime)
                            .increment(1);
                }
                if (arrivalDelay > 0 && departureDelay <= 0) {
                    this.mos.write("OnTimeDepDelayedArr", NullWritable.get(),
                            value);
                    context.getCounter(OnTimeStatistics.DepOnTimeArrDelayed)
                            .increment(1);
                }
                if (arrivalDelay > 0 && departureDelay > 0) {
                    this.mos.write("DelayedDepDelayedArr", NullWritable.get(),
                            value);
                    context.getCounter(OnTimeStatistics.DepDelayedArrDelayed)
                            .increment(1);
                }

                if (arrivalDelay > 0 || departureDelay > 0) {
                    context.getCounter("DELAY_FOR_PERIOD",
                            "MONTH_" + Integer.toString(dw.month.get()))
                            .increment(1);
                    context.getCounter("DELAY_FOR_PERIOD",
                            "DAY_OF_WEEK_" + Integer.toString(dw.month.get()))
                            .increment(1);
                }

            }

        }
    }

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(MultiOutputMRJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "OnTimeDepOnTimeArr",
                TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "DelayedDepOnTimeArr",
                TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "OnTimeDepDelayedArr",
                TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "DelayedDepDelayedArr",
                TextOutputFormat.class, NullWritable.class, Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MultiOutputMapper.class);
        job.setNumReduceTasks(0);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        this.printCounterGroup(job, "DELAY_FOR_PERIOD");
        this.printCounterGroup(job,
                "org.apress.prohadoop.c6.MultiOutputMRJob$MultiOutputMapper$OnTimeStatistics");

        return 0;
    }

    private void printCounterGroup(Job job, String groupName)
            throws IOException {
        CounterGroup cntrGrp = job.getCounters().getGroup(groupName);
        Iterator<Counter> cntIter = cntrGrp.iterator();
        System.out.println("\nGroup Name = " + groupName);
        while (cntIter.hasNext()) {
            Counter cnt = cntIter.next();
            System.out.println(cnt.getName() + "=" + cnt.getValue());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new MultiOutputMRJob(), args);
    }

}