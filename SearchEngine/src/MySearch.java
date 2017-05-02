

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.*;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MySearch
{
	public static class Mapperclass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		Text phoneNumber =new Text();
		IntWritable one = new IntWritable(1);
		
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try
	         {
	        	 
	            String[] parts = value.toString().split(",");
	            if(parts[4].equals("1"))
	            {
	      
	            
	         }
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	      
	      private long toMillis(String date)
	      {
	    	  SimpleDateFormat format= new SimpleDateFormat("YYYY-MM-dd HH:mm:ss") ; 
	      
	      Date dateFrm=null;
	      
	      try
	      {
	    	  
	    	  dateFrm =format.parse(date);
	      }
	      
	      catch(ParseException e)
	      {
	    	  e.printStackTrace();
	      }
	      return dateFrm.getTime();
	      
	   }
	   }
	public static class Reducerclass extends Reducer<Text, IntWritable, Text, IntWritable>
	{
	
		private IntWritable result=new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			long total=0;
			    for (IntWritable val : values)
			    {
			    total+=val.get();
			    }
			  result.set((int)total);
			if(total>=60);
			{
			context.write(key, result);
		}
	}
}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//conf.set("","")
		Job job = new Job(conf, "std count");

		job.setJarByClass(MySearch.class);

		job.setJobName("call by each number");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Mapperclass.class);
		job.setReducerClass(Reducerclass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	}
