package org.apress.prohadoop.utils;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class ZipArchiveToSeqFileConverter {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        SequenceFile.Writer writer = null;
        try{
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            
            ZipFile zipFile = new ZipFile(args[0]);
            Path seqFilePath = new Path(args[1]);
            writer = SequenceFile.createWriter(conf,
                    Writer.file(seqFilePath), Writer.keyClass(Text.class),
                    Writer.valueClass(Text.class));
            Text key = new Text("");
            Text value = new Text("");
            
            Enumeration zipEntries = zipFile.entries();
            while(zipEntries.hasMoreElements()){
                ZipEntry entry = (ZipEntry)zipEntries.nextElement();
                InputStream in = zipFile.getInputStream(entry);
                key.set(new Text(entry.getName()));
                byte[] fileContents = new byte[(int)entry.getSize()];
                
                in.read(fileContents, 0, (int)entry.getSize());
                value.set(fileContents);
                writer.append(key, value);
            }
            
        }

        finally{
            writer.close();    
        }    
        
        ZipConvertedSequenceFileReader.readFile(args[1]);
    }
}
