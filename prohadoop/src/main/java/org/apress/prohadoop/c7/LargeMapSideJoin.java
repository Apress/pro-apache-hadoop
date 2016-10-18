package org.apress.prohadoop.c7;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class LargeMapSideJoin extends Configured implements Tool{

 public static class LargeMapSideJoinMapper extends Mapper<Text, TupleWritable, Text, Text> {
    public void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
		//Key is the join key
        Text connectingFlight = new Text("");
        if (value.toString().length() > 0) {
            String originAtts[] = value.get(0).toString().split(",");
            String destAtts[] = value.get(1).toString().split(",");
            String oFN = AirlineDataUtils.getFlightNum(originAtts);
            String oDT = AirlineDataUtils.getDepartureTime(destAtts);
            String dFN = AirlineDataUtils.getFlightNum(destAtts);
            String dAT = AirlineDataUtils.getArrivalTime(destAtts);
            //Add logic to ensure originDepTime > depArrTime
            connectingFlight.set(key+","+oFN+","+oDT+","+dFN+","+dAT);
            context.write(key, connectingFlight);
        }
    } 
 }

 

public  int run(String[] allArgs) throws Exception {
    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
    
	Job job = Job.getInstance(getConf());
    job.setJarByClass(LargeMapSideJoin.class);
    job.setInputFormatClass(CompositeInputFormat.class);
    Path flightByDtAndOrigin = new Path(args[0]);
    Path flightByDtAndDestination = new Path(args[1]);
    
    String strJoinStmt = CompositeInputFormat.compose("inner",
            KeyValueTextInputFormat.class, flightByDtAndOrigin, flightByDtAndDestination);
    job.getConfiguration().set("mapred.join.expr", strJoinStmt);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setMapperClass(LargeMapSideJoinMapper.class);
    job.setNumReduceTasks(0);

    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion(true);
    return 0;
 }

 public static void main(String[] args) throws Exception {
    ToolRunner.run(new LargeMapSideJoin(), args);
 }

}