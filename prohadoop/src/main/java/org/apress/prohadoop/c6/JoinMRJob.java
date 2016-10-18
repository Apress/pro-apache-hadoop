package org.apress.prohadoop.c6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class JoinMRJob extends Configured implements Tool {
    public static class FlightDataMapper extends
            Mapper<LongWritable, Text, CarrierKey, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                String[] contents = value.toString().split(",");
                String carrierCode = AirlineDataUtils
                        .getUniqueCarrier(contents);
                Text code = new Text(carrierCode.toLowerCase().trim());
                CarrierKey ck = new CarrierKey(CarrierKey.TYPE_DATA,code);
                       
                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(value.toString());
                Text dwTxt = AirlineDataUtils.parseDelaysWritableToText(dw);
                context.write(ck, dwTxt);
            }

        }
    }

    public static class CarrierMasterMapper extends
            Mapper<LongWritable, Text, CarrierKey, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if (!AirlineDataUtils.isCarrierFileHeader(value.toString())) {
                String[] carrierDetails = AirlineDataUtils
                        .parseCarrierLine(value.toString());
                Text carrierCode = new Text(carrierDetails[0].toLowerCase().trim());
                Text desc = new Text(carrierDetails[1].toUpperCase().trim());    
                CarrierKey ck = new CarrierKey(CarrierKey.TYPE_CARRIER, 
                                               carrierCode,
                                               desc);                           
                context.write(ck,new Text());
            }

        }
    }

    public static class JoinReducer extends
            Reducer<CarrierKey, Text, NullWritable, Text> {
        public void reduce(CarrierKey key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            String carrierDesc = "UNKNOWN";
            for (Text v : values) {
                if (key.type.equals(CarrierKey.TYPE_CARRIER)) {
                    carrierDesc = key.desc.toString();                   
                    continue;// Coming from the Master Data Mapper
                 }
                 else{
                 //The sorting comparator ensures carrierDesc was already set
                 //by the time this section is reached.
                    Text out = new Text(v.toString()+","+carrierDesc);
                    context.write(NullWritable.get(), out);
                 }
            }
        }

    }

    public static class CarrierCodeBasedPartitioner extends
            Partitioner<CarrierKey, Text> {
        @Override
        public int getPartition(CarrierKey key, Text value, int numPartitions) {
            return Math.abs(key.code.hashCode() % numPartitions);
        }
    }

    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        Job job = Job.getInstance(getConf());
        job.setJarByClass(JoinMRJob.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, FlightDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, CarrierMasterMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        
        job.setPartitionerClass(CarrierCodeBasedPartitioner.class);
        job.setSortComparatorClass(CarrierSortComparator.class);
        job.setGroupingComparatorClass(CarrierGroupComparator.class);
        
        job.setMapOutputKeyClass(CarrierKey.class);
        job.setMapOutputValueClass(Text.class);      
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(JoinReducer.class);

        
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new JoinMRJob(), args);
    }

}