package org.apress.prohadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apress.prohadoop.c6.DelaysWritable;

public class MapFileReader {

    public static void readFile(String fName) throws IOException {
        MapFile.Reader reader = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path seqFilePath = new Path(fName);

            reader = new MapFile.Reader(fs,fName,conf);

            LongWritable key = new LongWritable();
            DelaysWritable val = new DelaysWritable();
            
            reader.get(new LongWritable(1), val);
            System.err.println(key + "\t" + val.year + "/" + val.month
                    + "/" + val.date + " from/to==" + val.originAirportCode
                    + " to " + val.destAirportCode);
            
            
            reader.get(new LongWritable(11), val);
            System.err.println(key + "\t" + val.year + "/" + val.month
                    + "/" + val.date + " from/to==" + val.originAirportCode
                    + " to " + val.destAirportCode);
            reader.seek(new LongWritable(11));
            while (reader.next(key, val)) {
                System.err.println(key + "\t" + val.year + "/" + val.month
                        + "/" + val.date + " from/to==" + val.originAirportCode
                        + " to " + val.destAirportCode);
            }
            /*
            while (reader.next(key, val)) {
                System.err.println(key + "\t" + val.year + "/" + val.month
                        + "/" + val.date + " from/to==" + val.originAirportCode
                        + " to " + val.destAirportCode);
            }
            */

        } finally {
            reader.close();
        }

    }

    public static void main(String[] args) throws Exception {
        MapFileReader.readFile(args[0]);
    }

}
