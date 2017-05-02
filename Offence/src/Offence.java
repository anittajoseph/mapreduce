

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Offence 
{
	public static class MapClass extends Mapper<LongWritable,Text,Text,FloatWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try
	         {
	            String[] record = value.toString().split(",");	 
	            Long speed = Long.parseLong(record[2]);
	            context.write(new Text(record[1]),new FloatWritable(speed));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	public class ReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		FloatWritable offence = new FloatWritable();
		int offence_percent=0;
		
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException
		{
				
		    	
			    int offence_count=0;
			    int total=0;
			    for (FloatWritable val : values) 
			    
			    {
			   
				if (val.get() > 65) 
				{
					offence_count++;
				}
				total++;	
			}
			    offence_percent=(offence_count*100/total);
			offence.set(offence_percent);
			context.write(key, offence);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//conf.set("","")
		Job job = new Job(conf, "Speed_offence");

		job.setJarByClass(Offence.class);

		job.setJobName("vehicle speed");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	}

