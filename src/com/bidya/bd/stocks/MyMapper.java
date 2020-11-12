	
package com.bidya.bd.stocks;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

// common class for Double data type column
public class MyMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
{
	  protected Integer col;
      public void map(LongWritable key, Text value, Context context)
      {	    	  
         try{
            String[] str = value.toString().split(",");	 
            double val = Double.parseDouble(str[col]);
            context.write(new Text(str[Utils.STOCKS_ID]),new DoubleWritable(val));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
}

class MapperMax extends MyMapper
{
	public MapperMax() {
	    col = Utils.STOCKS_HIGH;
	}
}

class MapperMin extends MyMapper
{
	public MapperMin() {
	    col = Utils.STOCKS_LOW;
	}
}

class MapperClose extends MyMapper
{
	public MapperClose() {
	    col = Utils.STOCKS_CLOSE;
	}
}

//specific method for reading volume data (long)
class MapperVolume extends Mapper<LongWritable,Text,Text,LongWritable>
{
	protected Integer col;
	public MapperVolume() {
	    col = Utils.STOCKS_VOLUME;
	}
	
    public void map(LongWritable key, Text value, Context context)
    {	    	  
       try{
          String[] str = value.toString().split(",");	 
          long vol = Long.parseLong(str[col]);
          context.write(new Text(str[Utils.STOCKS_ID]),new LongWritable(vol));
       }
       catch(Exception e)
       {
          System.out.println(e.getMessage());
       }
    }
}

