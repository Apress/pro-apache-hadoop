package org.apress.prohadoop.c7;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.avro.FlightDelay;

public class AvroToTextFileConversionJob extends Configured implements Tool {

    public static class AvroToTextFileConversionMapper
            extends
            Mapper<AvroKey<NullWritable>, AvroValue<FlightDelay>, NullWritable, Text> {

        public void map(AvroKey<NullWritable> key,
                AvroValue<FlightDelay> value, Context context)
                throws IOException, InterruptedException {
            FlightDelay fd = value.datum();
            StringBuilder output = new StringBuilder("");
            output.append(fd.getYear().toString()).append(",")
                    .append(fd.getMonth().toString()).append(",")
                    .append(fd.getDate().toString()).append(",")
                    .append(fd.getDayOfWeek().toString()).append(",")
                    .append(fd.getArrDelay().toString()).append(",")
                    .append(fd.getDepDelay().toString()).append(",")
                    .append(fd.getOrigin().toString()).append(",")
                    .append(fd.getDestination().toString());

            context.write(NullWritable.get(), new Text(output.toString()));
        }
    }

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(AvroToTextFileConversionJob.class);
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job,
                Schema.create(org.apache.avro.Schema.Type.NULL));
        AvroJob.setInputValueSchema(job, FlightDelay.SCHEMA$);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(AvroToTextFileConversionMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new AvroToTextFileConversionJob(), args);
    }

}
// http://avro.apache.org/docs/1.7.6/mr.html
// -D xmlfile.start.tag=recs -D record.start.tag=rec ./airlinedataout/c6/xml
// ./airlinedataout/c6/seq
// airlinedata-m-00000.xml
// airlinedata-r-00000.xml