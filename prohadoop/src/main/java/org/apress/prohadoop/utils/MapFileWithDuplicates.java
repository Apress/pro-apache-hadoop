package org.apress.prohadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apress.prohadoop.c6.DelaysWritable;

public class MapFileWithDuplicates {

    public static void readFile(String fName) throws IOException {
        MapFile.Reader reader = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path seqFilePath = new Path(fName);

            reader = new MapFile.Reader(fs,fName,conf);

            LongWritable key = new LongWritable();
            Text val = new Text();
            
            reader.get(new LongWritable(1), val);
            System.out.println(key + "\t" + val);
            
            
            reader.get(new LongWritable(11), val);
            System.out.println(key + "\t" + val);
            
            System.out.println("Now testing seek");
            reader.seek(new LongWritable(10));
            while (reader.next(key, val)) {
                System.out.println(key + "\t" + val);
            }
          
        } finally {
            reader.close();
        }

    }

    public static void writeFile(String fName) throws IOException {
        MapFile.Writer writer = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path fPath = new Path(fName);
            LongWritable key = new LongWritable();
            Text val = new Text();

            writer = new MapFile.Writer(conf,fPath, MapFile.Writer.keyClass(key.getClass()), MapFile.Writer.valueClass(val.getClass()));

            for (int i = 1; i <= 12; i++) {
                key.set(i);
                val = new Text(Integer.toString(i));
                writer.append(key, val);                
                writer.append(key, val);                
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
     
        MapFileWithDuplicates.writeFile(args[0]);
        MapFileWithDuplicates.readFile(args[0]);
    }

}
