package org.apress.prohadoop.c6;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apress.prohadoop.utils.AirlineDataUtils;

@SuppressWarnings("deprecation")
public class MapSideJoinMRJob3 extends Configured implements Tool {

    public static class MapSideJoinMapper extends
            Mapper<LongWritable, Text, NullWritable, Text> {
        private Map<String, String[]> airports = new HashMap<String, String[]>();
        private Map<String, String[]> carriers = new HashMap<String, String[]>();

        private FileSystem hdfs = null;

        public List<String> readLinesFromJobFS(Path p) throws Exception {
            List<String> ls = new ArrayList<String>();

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    this.hdfs.open(p)));
            String line;
            line = br.readLine();
            while (line != null) {
                line = br.readLine();
                if(line!=null)
                    ls.add(line);
            }
            return ls;
        }

        private void readAirports(URI uri) throws Exception {
            List<String> lines = readLinesFromJobFS(new Path(uri));
            for (String line : lines) {
                if (!AirlineDataUtils.isAirportMasterFileHeader(line)) {
                    String[] airportDetails = AirlineDataUtils
                            .parseAirportMasterLine(line);
                    airports.put(airportDetails[0], airportDetails);
                }
            }
        }

        private void readCarriers(URI uri) throws Exception {
            List<String> lines = readLinesFromJobFS(new Path(uri));
            for (String line : lines) {
                if (!AirlineDataUtils.isCarrierFileHeader(line)) {
                    String[] carrierDetails = AirlineDataUtils
                            .parseCarrierLine(line);
                    carriers.put(carrierDetails[0], carrierDetails);
                }
            }
        }

        @Override
        public void setup(Context context) {

            try {
                this.hdfs = FileSystem.get(context.getConfiguration());
                URI[] uris = context.getCacheFiles();
                for (URI uri : uris) {
                    if (uri.toString().endsWith("airports.csv")) {
                        this.readAirports(uri);
                    }
                    if (uri.toString().endsWith("carriers.csv")) {
                        this.readCarriers(uri);
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }

        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (!AirlineDataUtils.isHeader(value)) {
                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(value
                        .toString());
                String orginAirportCd = dw.originAirportCode.toString();
                String destAirportCd = dw.destAirportCode.toString();
                String carrierCd = dw.carrierCode.toString();
                String[] originAirport = this.airports.get(orginAirportCd);
                String[] destAirport = this.airports.get(destAirportCd);
                String[] carrier = this.carriers.get(carrierCd);

                String originAirportDesc = "";
                if (originAirport != null)
                    originAirportDesc = originAirport[1].replaceAll(",", "");

                String destAirportDesc = "";
                if (destAirport != null)
                    destAirportDesc = destAirport[1].replaceAll(",", "");

                String carrierDesc = "";
                if (carrier != null)
                    carrierDesc = carrier[1].replaceAll(",", "");
                Text outLine = AirlineDataUtils.parseDelaysWritableToText(dw,
                        originAirportDesc, destAirportDesc, carrierDesc);
                context.write(NullWritable.get(), outLine);
            }
        }
    }

    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        Job job = Job.getInstance(getConf());
        job.setJarByClass(MapSideJoinMRJob3.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapSideJoinMapper.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile((new File(args[2])).toURI());
        job.addCacheFile((new File(args[3])).toURI());

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(new MapSideJoinMRJob3(), args);
    }

}