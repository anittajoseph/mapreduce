package retail_age;


import java.io.*;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class AgewiseTopProduct
{
	public static class Mapclass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            String prod_id = str[5];
	            String sale = str[8];
	            String agegroup = str[2];
	   
	            context.write(new Text(prod_id), new Text(agegroup+","+sale));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }


	
	
	public static class MyPartition extends Partitioner<Text,Text>
{


    @Override
    public int getPartition(Text key, Text value, int numReduceTasks)
    {
       String[] str = value.toString().split(",");
       String agegroup=str[0].trim();
       
       if(agegroup.equals("A"))
       {
          return 0;
       }
     if(agegroup.equals("B"))
       {
          return 1;
       }
     if(agegroup.equals("C"))
       {
          return 2;
       }
       
       
      if(agegroup.equals("D"))
       {
          return 3;
       }
      if(agegroup.equals("E"))
       {
          return 4;
       }
        if(agegroup.equals("F"))
       {
          return 5;
       }
       if(agegroup.equals("G"))
       {
          return 6;
       }
       if(agegroup.equals("H"))
       {
          return 7;
       }
        if(agegroup.equals("I"))
       {
          return 8;
       }
       else
       {
          return 9;
       }
       
       
    }

}
	public class Reduceclass extends Reducer<Text,Text,NullWritable,Text>
	   {
		   private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	       long sum = 0;
	         String myValue = "";
	         String mySum = "";
	         String age="";
	         for (Text val : values)
	         {
	        	 String[] str = val.toString().split(",");
	         		sum +=Long.parseLong(str[1]);
	         	 age=str[0];
	         }
	        myValue = key.toString();
	        mySum = String.format("%d", sum);
	        myValue = myValue + "," +age+","+ mySum;
			
	        repToRecordMap.put(new Long(sum), new Text(myValue));
			
			if (repToRecordMap.size() > 5) 
				{
						repToRecordMap.remove(repToRecordMap.firstKey());
				}

	      }
   
			protected void cleanup(Context context) throws IOException,
			InterruptedException 
			{
			
				for (Text t : repToRecordMap.values()) 
				{
						context.write(NullWritable.get(), t);
				}
			}
	      
	   }
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Top Buyer");
	    job.setJarByClass(AgewiseTopProduct .class);
	    job.setMapperClass(Mapclass.class);
	    
	    job.setPartitionerClass(MyPartition.class);
	    job.setNumReduceTasks(10);
	    job.setReducerClass(Reduceclass.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}