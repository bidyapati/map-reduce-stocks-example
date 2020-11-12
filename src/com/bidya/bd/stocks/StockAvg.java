package com.bidya.bd.stocks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*

0 exchange name
1 stock id
2 dt
3 open
4 high
5 low
6 close
7 volume
8 adj_close

 */
public class StockAvg {
	  public static void main(String[] args) throws Exception {
		  //set column to pick
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Max Stock Value");
		    job.setJarByClass(StockAvg.class);
		    job.setMapperClass(MapperClose.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceAvg.class);
		    job.setNumReduceTasks(1);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
