package org.apress.prohadoop.c6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ArrivalFlightKeySortingComparator extends WritableComparator {
    public ArrivalFlightKeySortingComparator() {
        super(ArrivalFlightKey.class, true);
    }
    @Override
    public int compare(WritableComparable  a, WritableComparable  b) {
        ArrivalFlightKey first = (ArrivalFlightKey) a;
        ArrivalFlightKey second = (ArrivalFlightKey) b;
        if (first.destinationAirport.equals(second.destinationAirport)) {
            return first.arrivalDtTime.compareTo(second.arrivalDtTime);
        } else {
            return first.destinationAirport.compareTo(second.destinationAirport);
        }
    }
}