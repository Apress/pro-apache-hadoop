package org.apress.prohadoop.c7;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class XMLOutputFormat extends FileOutputFormat<LongWritable,Text> {

  protected static class XMLRecordWriter extends RecordWriter<LongWritable, Text> {
    private DataOutputStream out;
    public XMLRecordWriter(DataOutputStream out) throws IOException {
      this.out = out;
      out.writeBytes("<recs>\n");
    }
    private void writeTag(String tag,String value) throws IOException{
        out.writeBytes("<"+tag+">"+value+"</"+tag+">");
    }
    public synchronized void write(LongWritable key, Text value) throws IOException {
      out.writeBytes("<rec>");
      this.writeTag("key", Long.toString(key.get()));
      String[] contents = value.toString().split(",");
      
      String year = AirlineDataUtils.getYear(contents);
      this.writeTag("year", year);
      String month = AirlineDataUtils.getMonth(contents);
      this.writeTag("month", month);
      String date = AirlineDataUtils.getDateOfMonth(contents);
      this.writeTag("date", date);
      String dow = AirlineDataUtils.getDayOfTheWeek(contents);
      this.writeTag("dayofweek", dow);      
      String depDelay =Integer.toString(AirlineDataUtils.parseMinutes(AirlineDataUtils.getDepartureDelay(contents),0));
      this.writeTag("depdelay", depDelay);
      String arrDelay = Integer.toString(AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents),0));
      this.writeTag("arrdelay", arrDelay);
      
      String originAirport = AirlineDataUtils.getOrigin(contents);
      this.writeTag("origin", originAirport);
      String destAirport = AirlineDataUtils.getDestination(contents);
      this.writeTag("destination", destAirport);
      
      String carrier = AirlineDataUtils.getUniqueCarrier(contents);
      this.writeTag("carrier", carrier);
      out.writeBytes("</rec>\n");
    }

    public synchronized void close(TaskAttemptContext job) throws IOException {
      try {
        out.writeBytes("</recs>\n");
      } finally {
        out.close();
      }
    }
  }

  public RecordWriter<LongWritable,Text> getRecordWriter(TaskAttemptContext job
                                   ) throws IOException {
    String extension = ".xml";
    Path file = getDefaultWorkFile(job, extension);
    FileSystem fs = file.getFileSystem(job.getConfiguration());
    FSDataOutputStream fileOut = fs.create(file, false);
    return new XMLRecordWriter(fileOut);
  }
  
}