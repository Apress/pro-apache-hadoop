package org.apress.prohadoop.c6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ArrivalFlightKeyGroupingComparator extends WritableComparator {    
    public ArrivalFlightKeyGroupingComparator() {
        super(ArrivalFlightKey.class, true);
    }
    /* Optional. If not provided the Key class compareTo method is invoked
     * Default implementation is as follows. Hence the custom key could
     * implement the sorting from a Grouping perspective
     *   public int compare(WritableComparable a, WritableComparable b) {
     *       return a.compareTo(b);
     *   }
     * 
     * */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // We could have simply used return a.compareTo(b);  See comment above
        ArrivalFlightKey first = (ArrivalFlightKey) a;
        ArrivalFlightKey second = (ArrivalFlightKey) b;        
        return first.destinationAirport.compareTo(second.destinationAirport);
    }
}