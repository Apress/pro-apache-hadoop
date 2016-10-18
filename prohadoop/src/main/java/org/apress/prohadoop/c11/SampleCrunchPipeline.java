package org.apress.prohadoop.c11;

import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SampleCrunchPipeline extends Configured implements Tool,Serializable{


public  int run(String[] allArgs) throws Exception {
    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
    boolean inMemory = Boolean.parseBoolean(args[0]);
    String inputPath = args[1];
    String outputPath = args[2];
    Pipeline pipeline = null;
    if(inMemory){
        pipeline = MemPipeline.getInstance();        
    }
    else{
        pipeline = new MRPipeline(SampleCrunchPipeline.class, getConf());;
    }
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
        @Override
        public void process(String line, Emitter<String> emitter) {
        for (String word : line.split("\\s+")) {
          emitter.emit(word);
        }
      }
    }, Writables.strings()); 
    PTable<String, Long> counts = words.count();
    pipeline.writeTextFile(counts,outputPath);
    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();    
    ToolRunner.run(new SampleCrunchPipeline(), args);
 }

}