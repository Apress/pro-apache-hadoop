package org.apress.prohadoop.c6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CarrierSortComparator extends WritableComparator {
    public CarrierSortComparator() {

        super(CarrierKey.class, true);

    }

}