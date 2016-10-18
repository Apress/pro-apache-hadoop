package org.apress.prohadoop.c7;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apress.prohadoop.c6.DelaysWritable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class XMLDelaysRecordReader extends RecordReader<LongWritable, DelaysWritable> {

    public static final String FILE_START_TAG_KEY = "xmlfile.start.tag";
    public static final String RECORD_START_TAG_KEY = "record.start.tag";

    private byte[] xmlFileTag;
    private byte[] recordTag;
    private byte[] recordEndTag;
    
    private String recordTagName = "";
    private long start;
    private long end;

    private FileSplit fileSplit;
    private Configuration conf;
    private FSDataInputStream in = null;
    private LongWritable key = new LongWritable(-1);
    private DelaysWritable value = new DelaysWritable();
    private DataOutputBuffer buffer = new DataOutputBuffer();

    public void initialize(InputSplit inputSplit,
            TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        xmlFileTag = taskAttemptContext.getConfiguration()
                .get(FILE_START_TAG_KEY).getBytes();
        recordTag = ("<"
                + taskAttemptContext.getConfiguration().get(
                        RECORD_START_TAG_KEY) + ">").getBytes();
        recordTagName = taskAttemptContext.getConfiguration().get(RECORD_START_TAG_KEY);
        recordEndTag = ("</"
                + taskAttemptContext.getConfiguration().get(
                        RECORD_START_TAG_KEY) + ">").getBytes();

        this.fileSplit = (FileSplit) inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        FileSystem fs = fileSplit.getPath().getFileSystem(conf);
        this.in = fs.open(fileSplit.getPath());
        this.in.seek(start);
        readUntilMatch(xmlFileTag, false);
    }

    public boolean nextKeyValue() throws IOException {
        if (this.in.getPos() < this.end && readUntilMatch(recordTag, false)) {
            buffer.write(this.recordTag);
            if (readUntilMatch(this.recordEndTag, true)) {
                key.set(key.get() + 1);
                DelaysWritable dw = this.parseDelaysWritable(this.createInputStream(buffer.getData()));
                value.setDelaysWritable(dw);
                this.buffer = new DataOutputBuffer();
                return true;
            }
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public DelaysWritable getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        float f = (float)(this.in.getPos()-this.start)/(float)(this.end-this.start);

        return f;
    }

    @Override
    public void close() throws IOException {
        this.in.close();
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock)
            throws IOException {
        int i = 0;
        
        while (true) {
            int b = this.in.read();
            // end of file:
            if (b == -1) {
                return false;
            }
            // save to buffer:
            if (withinBlock) {
                buffer.write(b);
            }

            // check if we're matching:
            if (b == match[i]) {
                i++;
                if (i >= match.length) {
                    return true;
                }
            } else {
                i = 0;
            }
            // see if we've passed the stop point:
            if (!withinBlock && i == 0 && this.in.getPos() >= end) {
                return false;
            }
        }
    }
    
    private DelaysWritable parseDelaysWritable(InputStream delaysXML) {
        try{
            DelaysWritable dw = new DelaysWritable();
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(delaysXML);
            NodeList nList = doc.getElementsByTagName(this.recordTagName);
            Element element = (Element)nList.item(0);
            
            dw.year=new IntWritable(Integer.parseInt(element.getElementsByTagName("year").item(0).getTextContent()));
            dw.month=new IntWritable(Integer.parseInt(element.getElementsByTagName("month").item(0).getTextContent()));
            dw.date=new IntWritable(Integer.parseInt(element.getElementsByTagName("date").item(0).getTextContent()));
            dw.dayOfWeek=new IntWritable(Integer.parseInt(element.getElementsByTagName("dayofweek").item(0).getTextContent()));
            dw.arrDelay=new IntWritable(Integer.parseInt(element.getElementsByTagName("arrdelay").item(0).getTextContent()));
            dw.depDelay=new IntWritable(Integer.parseInt(element.getElementsByTagName("depdelay").item(0).getTextContent()));
            dw.destAirportCode=new Text(element.getElementsByTagName("destination").item(0).getTextContent());
            dw.originAirportCode=new Text(element.getElementsByTagName("origin").item(0).getTextContent());
            dw.carrierCode=new Text(element.getElementsByTagName("carrier").item(0).getTextContent());
            return dw;
        }
        catch(Exception ex){
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }

    }
    private InputStream createInputStream(byte[] bytes)
            throws java.io.UnsupportedEncodingException {
            String xml = (new String(bytes)).trim();
            //xml = xml.substring(0,xml.indexOf(new String(this.recordEndTag))+(new String(this.recordEndTag)).length());

            //return new ByteArrayInputStream(xml.getBytes());
        return new ByteArrayInputStream(xml.getBytes());
    }

}