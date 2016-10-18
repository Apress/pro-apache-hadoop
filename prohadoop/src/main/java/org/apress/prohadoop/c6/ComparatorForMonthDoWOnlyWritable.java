package org.apress.prohadoop.c6;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;


public  class ComparatorForMonthDoWOnlyWritable implements RawComparator {
    MonthDoWOnlyWritable first = null;
    MonthDoWOnlyWritable second = null;
    DataInputBuffer buffer = null;
    public ComparatorForMonthDoWOnlyWritable(){
        first = new MonthDoWOnlyWritable();
        second = new MonthDoWOnlyWritable();
        buffer = new DataInputBuffer();
    }
    Text.Comparator x = null;
    
    public int compare(Object a, Object b) {
        MonthDoWOnlyWritable first = (MonthDoWOnlyWritable)a;
        MonthDoWOnlyWritable second = (MonthDoWOnlyWritable)b;
        if(first.month.get()!=second.month.get()){
            return first.month.compareTo(second.month);
        }
        if(first.dayOfWeek.get()!=second.dayOfWeek.get()){
            return -1 * first.dayOfWeek.compareTo(second.dayOfWeek);
        }
        return 0;
      }

    
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        // TODO Auto-generated method stub
        try {
            buffer.reset(b1, s1, l1);                  
            first.readFields(buffer);
            
            buffer.reset(b2, s2, l2);                   
            second.readFields(buffer);
            
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        return this.compare(first,second);
    }
}