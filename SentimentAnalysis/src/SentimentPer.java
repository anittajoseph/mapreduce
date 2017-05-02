

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class SentimentPer {
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		private final static IntWritable totalval= new IntWritable();
	    private Text word = new Text();
	   int myvalue=0;
	   String myword="";
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		   // URI[] files = context.getCacheFiles(); // getCacheFiles returns null
			 URI[] files =DistributedCache.getCacheFiles(context.getConfiguration());

		    Path p = new Path(files[0].toString());
		
			if (p.getName().equals("AFINN.txt")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String diction_word = tokens[0];
						String diction_value = tokens[1];
						abMap.put(diction_word, diction_value);
						line = reader.readLine();
					}
					reader.close();
				}
			
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load dictionary data.");
			}
		}

			
		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	StringTokenizer itr= new StringTokenizer(value.toString());
        	
        	while(itr.hasMoreTokens())
        	{
        		myword= itr.nextToken().toLowerCase();
        		if(abMap.get(myword) !=null)
        		{
        			myvalue=Integer.parseInt(abMap.get(myword));
        			if(myvalue>0)
        			{
        				myword= "positive";
        			
        			}
        			if(myvalue<0)
        			{
        				myword="negative";
        				myvalue=myvalue* -1;
        			}
        		}
        		else
        		{
        			myword="positive";
        			myvalue=0;
        		}
        		word.set(myword);
        		totalval.set(myvalue);
        		context.write(word, totalval);
        	
        	}
        } 
        
        }
	public static class IntReducer extends
	Reducer<Text, IntWritable, NullWritable, Text> {
		int postotal=0;
		int negtotal=0;
		double sentpercent= 0.00;
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable val:values)
			{
				sum+=val.get();
			}
			if(key.toString().equals("positive"))
			{
				postotal= sum;
			}
			if(key.toString().equals("negative"))
			{
				negtotal=sum;
			}
		}
	
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
		
			sentpercent= (((double)postotal)-((double)negtotal))/(((double)postotal) +((double)negtotal)) *100;
			String str= "Sentiment percent for the given text is " + String.format("%f", sentpercent);
			context.write(NullWritable.get(), new Text (str));
			}
		}
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	//conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = new Job(conf, "sentiment %");
    job.setJarByClass(SentimentPer.class);
    job.setJobName("Sentiment percent");
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(IntReducer.class);
    //job.addCacheFile(new Path("AFINN.txt").toUri());
    
    try
    {
    	DistributedCache.addCacheFile(new URI("AFINN.txt"),job.getConfiguration());
    }
    catch(Exception ex)
    {
    	System.out.println(ex.getMessage());
    }
    //job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    
  }
}
