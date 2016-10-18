package org.apress.prohadoop.c11;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


public class CustomIf extends FilterFunc {
    
    @Override
    public Boolean exec(Tuple input) throws IOException {       
        return CustomIf.isTrue(input);
    }
    
    protected static Boolean isTrue(Tuple input) throws IOException {        
        try {
            Object o = input.get(0);
            if (o instanceof DataBag) {
                DataBag db = (DataBag) o;
                Iterator it = db.iterator();
                while(it.hasNext()){
                    Object s = it.next();
                    
                    if(CustomIf.isTrue(s.toString())){
                        return true;
                    }
                }
            }
            else{                
                return CustomIf.isTrue(o.toString());
            }
        } catch (ExecException ee) {
            throw ee;
        }
        return false;
    }

    protected static Boolean isTrue(String s){
        if(s.equals("A") || s.equals("B")){
            return true;
        }
        return false;
    }

} 
