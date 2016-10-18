package org.apress.prohadoop.c8;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

public class WordCountTestWithMiniCluster extends ClusterMapReduceTestCase {
    public static final String INPUT_STRING = "test\ntest\nword\ntest\njava\njob\n";
    Path inputPath = null;
    Path outputPath = null;
    Configuration conf = null;

    protected Map<String, Integer> getCountsByWord()
            throws Exception {
        Map<String, Integer> results = new HashMap<String, Integer>();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(outputPath);
        for (FileStatus file : fileStatus) {
            String name = file.getPath().getName();
            if (name.contains("part")) {
                Path outFile = new Path(outputPath, name);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(outFile)));
                String line;
                line = reader.readLine();
                while (line != null) {                    
                    String[] vals = line.split("\\t");
                    results.put(vals[0], Integer.parseInt(vals[1]));
                    line = reader.readLine();
                }
                reader.close();
            }
        }
        return results;
    }

    private void createFile(FileSystem fs, Path filePath) throws IOException {
        FSDataOutputStream out = fs.create(filePath);
        out.write(INPUT_STRING.getBytes());
        out.close();
    }

    private void prepareEnvironment() throws Exception {
        this.conf = this.createJobConf();
        this.inputPath = new Path("input/wordcount/");
        this.outputPath = new Path("output/wordcount/");
        FileSystem fs = FileSystem.get(conf);
        fs.delete(inputPath, true);
        fs.delete(outputPath, true);
        fs.mkdirs(inputPath);
        this.createFile(fs, new Path(inputPath, "test.txt"));

    }

    private Job configureJob() throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountTestWithMiniCluster.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }


    
    @Test
    public void testWordCount() throws Exception {
        this.prepareEnvironment();
        Job job = this.configureJob();
        boolean status = job.waitForCompletion(true);
        assertTrue(job.isSuccessful());
        Map<String, Integer> countsByWord = getCountsByWord();
        assertEquals(new Integer(1), countsByWord.get("java"));
        assertEquals(new Integer(1), countsByWord.get("job"));
        assertEquals(new Integer(3), countsByWord.get("test"));
        assertEquals(new Integer(1), countsByWord.get("word"));
    }
    

}