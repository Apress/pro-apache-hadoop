package org.apress.prohadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apress.prohadoop.c6.DelaysWritable;

public class SequenceFileReader {

    public static void readFile(String fName) throws IOException {
        SequenceFile.Reader reader = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path seqFilePath = new Path(fName);

            reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));

            LongWritable key = new LongWritable();
            DelaysWritable val = new DelaysWritable();

            while (reader.next(key, val)) {
                System.err.println(key + "\t" + val.year + "/" + val.month
                        + "/" + val.date + " from/to==" + val.originAirportCode
                        + " to " + val.destAirportCode);
            }

        } finally {
            reader.close();
        }

    }

    public static void main(String[] args) throws Exception {
        SequenceFileReader.readFile(args[0]);
    }

}
