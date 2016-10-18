package org.apress.prohadoop.c6;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ConfigurablePartitioner extends
        Partitioner<Writable, Text> implements Configurable {
    public static final String MIN_VAL="MIN_VAL";
    public static final String MAX_VAL="MAX_VAL";
    
    private long minIndex = 0;
    private long maxIndex = Long.MAX_VALUE;
    
    private long convertKeyToIndex(Writable key){
        //The hashcode function of your custom class can be implemented to provide an index
        return (long) key.hashCode();
    }    
    
    public int getPartition(Writable key, Text value, int numReduceTasks) {
        int partition=0;
        long index = this.convertKeyToIndex(key);
        //Logic to utilize the range, index, numReduceTasks to obtain partition id
        return partition;
    }

    
    public void setConf(Configuration conf) {
        //Implement logic to populate min and max
        minIndex = conf.getLong(MIN_VAL, 0);
        maxIndex = conf.getLong(MAX_VAL, 0);        
    }

    
    public Configuration getConf() {
        // TODO Auto-generated method stub
        return null;
    }
}
