package com.bidya.bd.temp;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Fnd max per year
Year and temperature
1900 39
1900 14
1900 5
1900 11
1900 20
1900 20
1900 22
1900 15
1900 41
1900 42
1900 46
1900 6
1900 13
 */
public class MaxTemperature {

  public static class MaxTempMapper
       extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] yrTemp = value.toString().split(" ");
        if (yrTemp.length < 2) {
        	return;
        }
        
        Integer year = Integer.parseInt(yrTemp[0]);
        Integer temp = Integer.parseInt(yrTemp[1]);
        IntWritable yearInt = new IntWritable(year);
        IntWritable TempInt = new IntWritable(temp);
        context.write(yearInt, TempInt);
    }
  }

  public static class MaxTempReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int max = 0;
        for (IntWritable val : values) {
          max = max < val.get() ? val.get() : max;
        }
      
	    result.set(max);
      	context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Max Temp Calculator");
    job.setJarByClass(MaxTemperature.class);
    job.setMapperClass(MaxTempMapper.class);
    job.setReducerClass(MaxTempReducer.class);
    job.setNumReduceTasks(1);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
