package org.apress.prohadoop.c7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apress.prohadoop.c6.DelaysWritable;

public class AdvancedGroupBySumMapper extends
        Mapper<LongWritable, DelaysWritable, IntWritable, LongWritable> {

    public void map(LongWritable key, DelaysWritable value, Context context)
            throws IOException, InterruptedException {
        //Do nothing

    }
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        IntWritable month = new IntWritable(0);
        LongWritable total = new LongWritable(0); 
        while (context.nextKeyValue()) {
          LongWritable key = context.getCurrentKey();
          DelaysWritable dw = context.getCurrentValue();
          if(month.get()==dw.month.get()){              
              total.set(total.get()+1);              
          }
          else{
              if(month.get()>0){//Skip the first iteration
                context.write(month, total);//Write intermediate total
              }
              month = new IntWritable(dw.month.get());
              //Reset total to one as it is first record
              total = new LongWritable(1);               
          }          
        }
        //Write the last aggregation for this file if there were any 
        //records which is indicated by month.get() being > 0 
        if(month.get()>0){
            context.write(month, total);
        }        
        cleanup(context);
    }
}