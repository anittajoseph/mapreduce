

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyStringSearch
{
	public static class Mapperclass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		
		private final static IntWritable one = new IntWritable(1);
		private Text scentence =new Text();
		
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	      {	    	  
	         
	        	 
	            String mySearchText = context.getConfiguration().get("myText");
	            String line = value.toString();
	           
	            if(mySearchText != null)
	            {
	            	if(line.contains(mySearchText))
	            	{
	            		scentence.set(line);
	            		context.write(scentence, one);
	            	}
	            }
	         
	      	}
	     }
	      
	    
	public static class Reducerclass extends Reducer<Text, IntWritable, Text, IntWritable>
	{
	
		private IntWritable result=new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			long sum=0;
			    for (IntWritable val : values)
			    {
			    sum +=val.get();
			    }
			  result.set((int) sum);
			
			context.write(key, result);
		
	}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//conf.set("","")
		if(args.length > 2)
		{
			conf.set("MyText",args[2]);
		}
		Job job = new Job(conf, "String search");

		job.setJarByClass(MyStringSearch.class);

		//job.setJobName("search value");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Mapperclass.class);
		job.setReducerClass(Reducerclass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	}
