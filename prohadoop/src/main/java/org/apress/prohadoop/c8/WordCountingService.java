package org.apress.prohadoop.c8;
import org.apache.hadoop.io.Text;
public class WordCountingService {
    private Text word=null;
    private int count=0;
    
    public WordCountingService(Text word){
        this.word = word;
    }
    public void incrementCount(int incr){
        this.count=this.count+incr;
    }
    public Text getWord() {
        return word;
    }
    public int getCount() {
        return this.count;
    }
}
