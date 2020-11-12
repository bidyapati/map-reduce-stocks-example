package com.bidya.bd.patent;


import java.io.IOException;
import java.util.StringTokenizer;

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
Patent id and sub patent id
1 1.232
1 1.45
1 1.153
1 1.100
1 1.77
1 1.170
1 1.111
1 1.11
1 1.220
1 1.3
1 1.169
 */
public class SubPatentCount {

  public static class PatentMapper
       extends Mapper<LongWritable, Text, LongWritable, IntWritable>{

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] patents = value.toString().split(" ");
        if (patents.length < 2) {
        	return;
        }
        
        Long patentId = Long.parseLong(patents[0]);
        LongWritable patIdKey = new LongWritable(patentId);
        IntWritable one = new IntWritable(1);
        context.write(patIdKey, one);
    }
  }

  public static class PatentReducer
       extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(LongWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
      
	    result.set(sum);
      	context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sub patent count");
    job.setJarByClass(SubPatentCount.class);
    job.setMapperClass(PatentMapper.class);
    job.setReducerClass(PatentReducer.class);
    job.setNumReduceTasks(1);
    
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
