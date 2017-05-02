import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrowthCycle
{
	public static class GrowthMapper extends
	Mapper<LongWritable, Text, Text, Text>
	{
		public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException
		{
			try
			{
			
			String record = value.toString();
			String[] parts = record.split("\t");
			long jan=Long.parseLong(parts[1]);
			long feb=Long.parseLong(parts[2]);
			long nov=Long.parseLong(parts[3]);
			long dec=Long.parseLong(parts[4]);
			
			String MyKey = key.toString();
			
			float nov_dec=((dec-nov)*100/nov);
			float dec_jan=((dec-jan)*100/jan);
			float jan_feb=((jan-feb)*100/feb);
			
			double Avg=((nov_dec+dec_jan+jan_feb)/3);
			
			String cycle1=String.format("%f",nov-dec);
			String cycle2=String.format("%f",dec_jan);
			String cycle3=String.format("%f",jan_feb);
			String avgr=String.format("%f",Avg);
			
			String  myrow=cycle1+","+cycle2+","+cycle3+","+avgr;
			 
			context.write(new Text(MyKey), new Text(myrow));
			}
			
			catch(Exception e)
			{
				
				System.out.println(e);
			}
			
		}
	}
	
public static void main(String[] args) throws Exception 
{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
	    job.setJarByClass(GrowthCycle .class);
	    job.setJobName("Reduce Side Join");
	    job.setNumReduceTasks(0);
	    job.setMapperClass(GrowthMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
	
}
