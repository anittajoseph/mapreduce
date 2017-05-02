
 
import java.io.*;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Reduceclass extends Reducer<Text,Text,NullWritable,Text>
	   {
		   private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	       long sum = 0;
	         String myValue = "";
	         String mySum = "";
	         for (Text val : values)
	         {
	        	 String[] str = val.toString().split(",");
	         		sum +=Long.parseLong(str[1]);
	         	 
	         }
	        myValue = key.toString();
	        mySum = String.format("%d", sum);
	        myValue = myValue + ',' + mySum;
			
	        repToRecordMap.put(new Long(sum), new Text(myValue));
			
			if (repToRecordMap.size() > 5) 
				{
						repToRecordMap.remove(repToRecordMap.firstKey());
				}

	      }
      
			protected void cleanup(Context context) throws IOException,
			InterruptedException 
			{
			
				for (Text t : repToRecordMap.values()) 
				{
						context.write(NullWritable.get(), t);
				}
			}
	      
	   }