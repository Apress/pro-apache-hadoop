package org.apress.prohadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apress.prohadoop.c6.DelaysWritable;

public class SequenceFileWriter {

    public static void writeFile(String fName) throws IOException {
        SequenceFile.Writer writer = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path seqFilePath = new Path(fName);
            LongWritable key = new LongWritable();
            DelaysWritable val = new DelaysWritable();

            writer = SequenceFile.createWriter(fs, conf, seqFilePath,
                    key.getClass(), val.getClass());

            for (int i = 1; i <= 12; i++) {
                key.set(i);
                val.month = new IntWritable(i);
                writer.append(key, val);

            }
        } finally {
            writer.close();
        }

    }

    public static void main(String[] args) throws Exception {
        SequenceFileWriter.writeFile(args[0]);
    }

}
