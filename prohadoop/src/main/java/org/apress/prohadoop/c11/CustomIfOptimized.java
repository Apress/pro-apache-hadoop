package org.apress.prohadoop.c11;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class CustomIfOptimized extends CustomIf implements Algebraic, Accumulator<Boolean> {
    private boolean intermediate=false;

    @Override
    public void accumulate(Tuple arg) throws IOException {
        if(!intermediate){//If already true skip this batch
            intermediate = isTrue(arg);
        }
    }

    @Override
    public void cleanup() {
        this.intermediate=false;
    }

    @Override
    public Boolean getValue() {
        return intermediate;
    }

    public String getInitial() {
        return Initial.class.getName();
    }
    public String getIntermed() {
        return Intermed.class.getName();
    }
    public String getFinal() {
        return Final.class.getName();
    }
    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
          return TupleFactory.getInstance().newTuple(isTrue(input));
                
        }
    }
    
    static public class Intermed extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
           return TupleFactory.getInstance().newTuple(isTrue(input));
        }
    }
    
    static public class Final extends EvalFunc<Boolean> {
        public Boolean exec(Tuple input) throws IOException {
           return isTrue(input);
        }
    }
    
  

} 
