
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterSalesData
{
	public static class Mapperclass extends Mapper<LongWritable,Text,NullWritable,NullWritable>
	   {
		Text sentence =new Text();
		IntWritable one = new IntWritable(1);
		String MySearchText= 
				
	
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	      {	
	    	  for()
	    	  {
	    		  
	    		  
	    	  }
	    	if (MySearchText!=null) 
	    	{
	    		
	    		
	    	}
	         
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
