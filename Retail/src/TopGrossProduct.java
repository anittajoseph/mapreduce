
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

  
public class TopGrossProduct extends Configured implements Tool 
{
	
	
	
	 
	   public int run(String[] arg) throws Exception 
	   {
			
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Top Buyer");
		    job.setJarByClass(TopGrossProduct .class);
		    job.setMapperClass(Mapclass.class);
		    
		    job.setPartitionerClass(MyPartition.class);
		   
		    job.setReducerClass(Reduceclass.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(arg[0]));
		    FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    
		    return 0;
		  }
	   
	   
	   public static void main(String args[] ) throws Exception
	   {
		   
		   ToolRunner.run(new Configuration(), new TopGrossProduct(),args);
	   }

	

}
