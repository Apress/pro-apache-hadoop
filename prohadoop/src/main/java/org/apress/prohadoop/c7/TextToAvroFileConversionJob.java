package org.apress.prohadoop.c7;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.avro.FlightDelay;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class TextToAvroFileConversionJob extends Configured implements Tool {

    public static class TextToAvroFileConversionMapper extends
            Mapper<LongWritable, Text, AvroKey<NullWritable>, AvroValue<FlightDelay>> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if(!AirlineDataUtils.isHeader(value)){         
                String[] contents = value.toString().split(",");
                FlightDelay.Builder fd =  FlightDelay.newBuilder();
                
                int year = Integer.parseInt(AirlineDataUtils.getYear(contents));
                fd.setYear(year);
                
                int month = Integer.parseInt(AirlineDataUtils.getMonth(contents));
                fd.setMonth(month);
                
                int date = Integer.parseInt(AirlineDataUtils.getDateOfMonth(contents));
                fd.setDate(date);
                
                int dow = Integer.parseInt(AirlineDataUtils.getDayOfTheWeek(contents));
                fd.setDayOfWeek(dow);
                
                int arrDelay = AirlineDataUtils.parseMinutes(
                                   AirlineDataUtils.getArrivalDelay(contents),0);
                fd.setArrDelay(arrDelay);
                
                int depDelay = AirlineDataUtils.parseMinutes(
                                   AirlineDataUtils.getDepartureDelay(contents),0);
                fd.setDepDelay(depDelay);
                
                String origin = AirlineDataUtils.getOrigin(contents);
                fd.setOrigin(origin);
                
                String destination = AirlineDataUtils.getDestination(contents);
                fd.setDestination(destination);
                
                String carrier = AirlineDataUtils.getUniqueCarrier(contents);
                fd.setCarrier(carrier);
                
                context.write(new AvroKey<NullWritable>(NullWritable.get()), 
                              new AvroValue<FlightDelay>(fd.build()));
            }
        }
    }

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(TextToAvroFileConversionJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job,   Schema.create(org.apache.avro.Schema.Type.NULL));
        AvroJob.setOutputValueSchema(job, FlightDelay.SCHEMA$);
        job.setMapperClass(TextToAvroFileConversionMapper.class);       
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
        ToolRunner.run(new TextToAvroFileConversionJob(), args);
    }

}
//http://hadooped.blogspot.com/2013/09/sequence-file-construct-usage-code.html
//-D xmlfile.start.tag=recs -D record.start.tag=rec ./airlinedataout/c6/xml ./airlinedataout/c6/seq
//airlinedata-m-00000.xml
//airlinedata-r-00000.xml