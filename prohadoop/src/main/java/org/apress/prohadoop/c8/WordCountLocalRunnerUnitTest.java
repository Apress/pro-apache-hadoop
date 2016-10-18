package org.apress.prohadoop.c8;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

public class WordCountLocalRunnerUnitTest extends TestCase {
    
   private Job job = null;   
   private Path inputPath  = null;
   private Path outputPath = null;
   
   private Map<String, Integer> getCountsByWord() throws Exception {
       Map<String, Integer> countsByWord = new HashMap<String, Integer>();
       File outDir = new File(outputPath.toUri() + "/");       
       Collection<File> files = FileUtils.listFiles(outDir,
               TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
       
       for (File f : files) {
           if (f.getName().startsWith("part")
                   && !f.getAbsolutePath().endsWith("crc")) {
               List<String> lines = FileUtils.readLines(f);
               System.out.println(lines.size());
               for (String l : lines) {
                   String[] counts = l.split("\\t");
                   countsByWord.put(counts[0], Integer.parseInt(counts[1]));
               }
               break;
           }
       }
       return countsByWord;
   }

   private void configureJob(Configuration conf, Path inputPath, Path outputPath)
           throws Exception {
       this.job = Job.getInstance(conf);
       job.setJarByClass(WordCountLocalRunnerUnitTest.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       job.setMapperClass(WordCountMapper.class);
       job.setReducerClass(WordCountReducer.class);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.setInputPaths(job, inputPath);
       FileOutputFormat.setOutputPath(job, outputPath);
       
   }
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    public void setUp() throws Exception{
        this.inputPath = new Path("src/main/resources/input/wordcount/");
        this.outputPath = new Path("src/main/resources/output/wordcount/");
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "file:////");
       
        FileSystem fs = FileSystem.getLocal(conf);
        if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
        }

        this.configureJob(conf, inputPath, outputPath);
    }

    /**
     * @param conf
     * @param outputPath
     * @throws Exception
     */
    private void prepareEnvironment(Configuration conf, Path outputPath)
            throws Exception {
        FileSystem fs = FileSystem.getLocal(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    @Test
    public void testWordCount() throws Exception {
        boolean status = job.waitForCompletion(true);
        assertTrue(job.isSuccessful());                 
        Map<String, Integer> countsByWord = getCountsByWord();        
        assertEquals(new Integer(1),countsByWord.get("java"));
        assertEquals(new Integer(1),countsByWord.get("job"));
        assertEquals(new Integer(3),countsByWord.get("test"));
        assertEquals(new Integer(1),countsByWord.get("word"));
    }

}
