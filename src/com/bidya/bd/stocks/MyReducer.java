package com.bidya.bd.stocks;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


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

//common reducer for Double data type
public class MyReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
{
	    protected Integer task;
	    
	    
	    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
	      double out = 0;
		  long count = 0;// count for average
		  boolean firstData = true;// initialize first out if min calculation
		  
		  /**
		  List<DoubleWritable> dataLst = new ArrayList<DoubleWritable>();
		  values.forEach(dataLst::add);
		  DoubleWritable dwMax = Collections.max(dataLst, Comparator.comparing(DoubleWritable::get));
		  if (task == Utils.TASK_MAX) {	      
		      context.write(key, dwMax);
		      return;
     	  }
     	  **/
		  
         for (DoubleWritable val : values)
         {   count++;
		     if(firstData) {
		    	 out = val.get();
			     firstData = false;
			     continue;
		     }
		     
        	 if(task == Utils.TASK_SUM) {
        	     out += val.get();   
        	 } else if (task == Utils.TASK_MAX) {
        		 out = val.get()>out?val.get():out;
        	 } else if (task == Utils.TASK_MIN) {
        		 out = val.get()<out?val.get():out;
        	 } else if (task == Utils.TASK_AVG) {
        		 out += val.get();   
        	 }
         }
         
         if (task == Utils.TASK_AVG) {
    		 out = out/count;   
    	 }
         
 	      DoubleWritable result = new DoubleWritable();
	      result.set(out);		      
	      context.write(key, result);

	    }
}

class ReduceMax extends MyReducer
{
	public ReduceMax() {
		task = Utils.TASK_MAX;
	}
}

class ReduceMin extends MyReducer
{
	public ReduceMin() {
		task = Utils.TASK_MIN;
	}
}

class ReduceAvg extends MyReducer
{
	public ReduceAvg() {
		task = Utils.TASK_AVG;
	}
	
}


// special class for long data type
class ReduceSum  extends Reducer<Text,LongWritable,Text,LongWritable>
{
    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
	   long out = 0;

       for (LongWritable val : values) {   
      	    out += val.get();   
 
       }
       
       LongWritable result = new LongWritable();
	   result.set(out);		      
	   context.write(key, result);

	}
}

