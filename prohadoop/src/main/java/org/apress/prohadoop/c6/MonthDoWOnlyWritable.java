package org.apress.prohadoop.c6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class MonthDoWOnlyWritable implements Writable{
   
    
    public IntWritable month=new IntWritable();
    public IntWritable dayOfWeek = new IntWritable();
 
    public MonthDoWOnlyWritable(){ 
    }


    public void write(DataOutput out) throws IOException {
        this.month.write(out);
        this.dayOfWeek.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.month.readFields(in);
        this.dayOfWeek.readFields(in);
    }

   
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MonthDoWOnlyWritable)) {
          return false;
        }
        MonthDoWOnlyWritable other = (MonthDoWOnlyWritable)o;
        return this.month.get() == other.month.get() && this.dayOfWeek.get() == other.dayOfWeek.get();         
      }
    
    @Override
    public int hashCode() {
        int result = 0;
        result = (int)(this.month.get() ^ (this.month.get() >>> 32));
        result = 37 * result + (int)(this.dayOfWeek.get() ^ (this.dayOfWeek.get() >>> 32));
        return result;
    }
}

