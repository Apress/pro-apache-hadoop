package org.apress.prohadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apress.prohadoop.c6.DelaysWritable;

public class MapFileWriter {

    public static void writeFile(String fName) throws IOException {
        MapFile.Writer writer = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path seqFilePath = new Path(fName);
            LongWritable key = new LongWritable();
            DelaysWritable val = new DelaysWritable();

            writer = new MapFile.Writer(conf,seqFilePath, MapFile.Writer.keyClass(key.getClass()), MapFile.Writer.valueClass(val.getClass()));

            for (int i = 1; i <= 12; i++) {
                key.set(i);
                val.month = new IntWritable(i);
                writer.append(key, val);
                val.month = new IntWritable(i+1);
                writer.append(key, val);
                val.month = new IntWritable(i+2);
                writer.append(key, val);

            }
        } 
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally {
            writer.close();
        }

    }

    public static void main(String[] args) throws Exception {
        MapFileWriter.writeFile(args[0]);
    }

}
