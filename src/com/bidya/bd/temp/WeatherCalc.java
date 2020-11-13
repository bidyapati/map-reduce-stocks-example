package com.bidya.bd.temp;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Find hot and cold days (date)
 * 2nd column is the date
 * 6th column is max temperature and 7th column is min temperature
63891 20130101  5.102  -86.61   32.85    12.8     9.6    11.2    11.6    19.4 -9999.00 U -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
63891 20130102  5.102  -86.61   32.85    10.1     3.8     7.0     6.2     0.4 -9999.00 U -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
63891 20130103  5.102  -86.61   32.85     7.0    -2.2     2.4     2.9     0.0 -9999.00 U -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
 
 * Only mapper is required here.
 *
 */
public class WeatherCalc {

  public static class WeatherMapper
       extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	
        int i = 0;
        Double max = 0d;
        Double min = 0d;
        String dateTmp = "";
        while (itr.hasMoreTokens()) {
        	String cell = itr.nextToken();
        	if (i == 1) {
        		dateTmp = cell;
        	} else if(i == 5) {
        		max = Double.parseDouble(cell);
        	} else if (i == 6) {
        		min = Double.parseDouble(cell);
        	}

          ++i;
        }
        
        String out = null;
        if (max > 40) {
        	out = "A Hot Day";
        } else if (min < 10) {
        	out = "A Cold Day";
        } else {
        	return;
        }
        
        Text dateWrite = new Text(dateTmp);
        Text res = new Text(out);
        context.write(dateWrite, res);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Weather Calculator");
    job.setJarByClass(WeatherCalc.class);
    job.setMapperClass(WeatherMapper.class);
    job.setNumReduceTasks(0);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
