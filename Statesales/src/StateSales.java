

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StateSales 
{
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try
	         {
	            String[] str = value.toString().split(",");	 
	          
	         int qty=Integer.parseInt(str[2]);
	         int price=Integer.parseInt(str[3]);
	          int amount =qty*price;
	          
	            context.write(new Text(str[4]),new IntWritable(amount));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	
	public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private Text outputKey = new Text();
		private IntWritable result =new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int sum=0;
			    for (IntWritable val : values) 
			    {
			    sum+= val.get();
			    }
			    result.set(sum);
			    context.write(outputKey, result);
		}
			  
	}
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//conf.set("","")
		Job job = new Job(conf, "");

		job.setJarByClass(StateSales.class);

		job.setJobName("state wise sales");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setCombinerClass(ReducerClass.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	}
