package org.apress.prohadoop.c6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CarrierGroupComparator extends WritableComparator {
    
    public CarrierGroupComparator() {

        super(CarrierKey.class, true);

    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CarrierKey first = (CarrierKey) a;
        CarrierKey second = (CarrierKey) b;
        return first.code.compareTo(second.code);
    }

}