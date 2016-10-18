package org.apress.prohadoop.c6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CarrierKey implements WritableComparable<CarrierKey> {
    public static final IntWritable TYPE_CARRIER = new IntWritable(0);
    public static final IntWritable TYPE_DATA = new IntWritable(1);
    public IntWritable type = new IntWritable(3);
    public Text code = new Text("");
    public Text desc = new Text("");

    public CarrierKey() {
    }

    public CarrierKey(IntWritable type, Text code,Text desc) {
        this.type = type;
        this.code = code;
        this.desc = desc;
    }
    public CarrierKey(IntWritable type, Text code) {
        this.type = type;
        this.code = code;
    }

    @Override
    public int hashCode() {
        return (this.code.toString()+ Integer.toString(this.type.get())).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CarrierKey))
            return false;
        CarrierKey other = (CarrierKey) o;
        return (this.code.equals(other.code) && this.type.equals(other.type));
    }

    
    public int compareTo(CarrierKey second) {
        
        CarrierKey first = this;
        if (first.code.equals(second.code)) {
            return first.type.compareTo(second.type);
        } else {
            return first.code.compareTo(second.code);
        }

    }
    
  
    public void write(DataOutput out) throws IOException {
        this.type.write(out);
        this.code.write(out);
        this.desc.write(out);
    }

  
    public void readFields(DataInput in) throws IOException {
        this.type.readFields(in);
        this.code.readFields(in);
        this.desc.readFields(in);
    }
}