package org.apress.prohadoop.c6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class MonthDoWWritable implements WritableComparable<MonthDoWWritable>{
    public int monthSort = 1;
    public int dowSort = -1;
    
    public IntWritable month=new IntWritable();
    public IntWritable dayOfWeek = new IntWritable();
 
    public MonthDoWWritable(){ 
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.month.write(out);
        this.dayOfWeek.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.month.readFields(in);
        this.dayOfWeek.readFields(in);
    }
    
    @Override
    public int compareTo(MonthDoWWritable second) {
        if(this.month.get()==second.month.get()){
            return -1*this.dayOfWeek.compareTo(second.dayOfWeek);
        }
        else{
            return 1*this.month.compareTo(second.month);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MonthDoWWritable)) {
          return false;
        }
        MonthDoWWritable other = (MonthDoWWritable)o;
        return this.month.get() == other.month.get() && this.dayOfWeek.get() == other.dayOfWeek.get();         
      }
    
    @Override
    public int hashCode() {
        return (this.month.get()-1);
    }
}

