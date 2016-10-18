package org.apress.prohadoop.c7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.c6.DelaysWritable;

public class XMLToSequenceFileConversionJob extends Configured implements Tool {

    public static class XMLToSequenceFileConversionMapper extends
            Mapper<LongWritable, DelaysWritable, LongWritable, DelaysWritable> {

        public void map(LongWritable key, DelaysWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);

        }
    }

  

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(XMLToSequenceFileConversionJob.class);
        job.setInputFormatClass(XMLDelaysInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DelaysWritable.class);

       
        job.setMapperClass(XMLToSequenceFileConversionMapper.class);       
        job.setNumReduceTasks(0);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
       
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
        //SequenceFileOutputFormat.setCompressOutput(job, true);
        //SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);


        //SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.BLOCK);
        //SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.RECORD);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new XMLToSequenceFileConversionJob(), args);
    }

}
//http://hadooped.blogspot.com/2013/09/sequence-file-construct-usage-code.html
//-D xmlfile.start.tag=recs -D record.start.tag=rec ./airlinedataout/c6/xml ./airlinedataout/c6/seq
//airlinedata-m-00000.xml
//airlinedata-r-00000.xml