package city;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctCities
{
	
	
	public static class DistinctMapper extends Mapper<LongWritable,Text,Text,NullWritable>
	   {
	      public void map(LongWritable key, Text value, Context context) 
	    		  throws IOException, InterruptedException
	      
	      {	    	  
	         try
	         {
	            String record = value.toString(); 
	            String[] parts = record.split(",");
	            String city=parts[3];
	            context.write(new Text(city),NullWritable.get());
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	
	public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable>
	{

		
		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException
		{
				
		    	context.write(key, NullWritable.get());
			 
			
		}
	}

	public static void main(String[] args) throws Exception
	{
		
		Configuration conf = new Configuration();
		//conf.set("","")
		Job job = new Job(conf,"distinct City");

		job.setJarByClass(DistinctCities.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(DistinctMapper.class);
		job.setReducerClass(DistinctReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
