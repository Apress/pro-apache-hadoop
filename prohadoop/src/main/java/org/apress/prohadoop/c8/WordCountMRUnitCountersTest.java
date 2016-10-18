package org.apress.prohadoop.c8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountMRUnitCountersTest extends TestCase {
    private Mapper mapper;
    private Reducer reducer;
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private MapReduceDriver mapReduceDriver;

    @Before
    public void setUp() {
        mapper = new WordCountMapper();
        mapDriver = new MapDriver(mapper);

        reducer = new WordCountWithCounterReducer();
        reduceDriver = new ReduceDriver(reducer);

        mapReduceDriver = new MapReduceDriver(mapper, reducer);
    }


    @Test
    public void testWordCountReducer() throws Exception {
        Text key = new Text("map");
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(key, values).withOutput(key, new IntWritable(2))
                .runTest();
        Assert.assertEquals(2,
                reduceDriver.getCounters().findCounter("FIRST_LETTER", "m")
                        .getValue());
        Assert.assertEquals(0,
                reduceDriver.getCounters().findCounter("FIRST_LETTER", "n")
                        .getValue());
    }

    @Test
    public void testWordCountMapReducer() throws Exception {
        this.mapReduceDriver.withInput(new LongWritable(1), new Text("map"))
                .withInput(new LongWritable(2), new Text("map"))
                .withInput(new LongWritable(3), new Text("reduce"))
                .withOutput(new Text("map"), new IntWritable(2))
                .withOutput(new Text("reduce"), new IntWritable(1)).runTest();

        assertEquals(2,
                     mapReduceDriver.getCounters()
                                    .findCounter("FIRST_LETTER", "m")
                                    .getValue());
        assertEquals(0,
                     mapReduceDriver.getCounters()
                                    .findCounter("FIRST_LETTER", "n")
                                    .getValue());

    }

}
