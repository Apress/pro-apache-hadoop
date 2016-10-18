package org.apress.prohadoop.c7;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apress.prohadoop.c6.DelaysWritable;
 
public class XMLDelaysInputFormat extends FileInputFormat<LongWritable, DelaysWritable> {
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	 
	@Override
	public RecordReader<LongWritable, DelaysWritable> createRecordReader(
		InputSplit split, TaskAttemptContext context) {
		return new XMLDelaysRecordReader();
	}
}