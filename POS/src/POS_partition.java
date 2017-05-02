

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

public class POS_partition 
{
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try
	         {
	            String[] str = value.toString().split(",");	 
	            String itemid = str[1];
	            String qty = str[2];
	             String state = str[4];
	           String myrow=qty+","+state;
	            context.write(new Text(itemid),new Text(myrow));
	            
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
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			int sum=0;
			    for (Text val : values) 
			    {
			    outputKey.set(key);
			    String [] str=val.toString().split(",");
			    sum+= Integer.parseInt(str[0]);
			    }
			    result.set(sum);
			    context.write(outputKey, result);
		}
			  
	}
	
	public static class CadePartitioner extends Partitioner <Text, Text>
	{
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
		String [] str=value.toString().split(",");
		if (str[1].equals("MAH"))
		{
			return 0;	
			
		}
		else
		{
			
			return 1;
		}
		
		}
	
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//conf.set("","")
		Job job = new Job(conf, "");

		job.setJarByClass(POS_partition.class);

		job.setJobName("total book writen by each writer");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	}
