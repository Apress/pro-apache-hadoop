package org.apress.prohadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

public class ZipConvertedSequenceFileReader {

    public static void readFile(String fName) throws IOException {
        SequenceFile.Reader reader = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path seqFilePath = new Path(fName);

            reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));

            Text key = new Text();
            Text val = new Text();

            while (reader.next(key, val)) {
                System.err.println(key);
                System.err.println(val.toString());
            }
        } finally {
            reader.close();
        }

        reader.close();
    }

    public static void main(String[] args) throws Exception {
        ZipConvertedSequenceFileReader.readFile(args[0]);
    }

}
