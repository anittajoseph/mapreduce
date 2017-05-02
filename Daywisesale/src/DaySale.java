import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DaySale
{

	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            String trndate = str[0];
	            
	            String trnDay=toDay(trndate);
	            long sale = Long.parseLong(str[8]);
	            context.write(new Text(trnDay), new LongWritable(sale));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	      
	      private String toDay(String date)
	      {
	    	  SimpleDateFormat format= new SimpleDateFormat("YYYY-MM-dd HH:mm:ss") ; 
	    	  
	    	  SimpleDateFormat newDateFormat= new SimpleDateFormat("EEEE");
	    	  
	      
	      Date dateFrm=null;
	      
	      try
	      {
	    	  
	    	  dateFrm =format.parse(date);
	      }
	      
	      catch(ParseException e)
	      {
	    	  e.printStackTrace();
	      }
	      return newDateFormat.format(dateFrm);
	      
	   }
	   }
	
	
	public static class ReduceClass extends Reducer<Text,LongWritable,Text,Text>
	   {
		   private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();
		   long grand_total=0;

	      public void reduce(Text key, Iterable <LongWritable> values, Context context) throws IOException, InterruptedException
	      {
	         long totalsales = 0;
	         String myValue = "";
	         String mytotal = "";
	         for (LongWritable val : values)
	         {
	         	totalsales += val.get();
	         	grand_total+=val.get();
	         	 
	         }
	        myValue = key.toString();
	        mytotal = String.format("%d", totalsales);
	        myValue = myValue + ',' + mytotal;
			
	        repToRecordMap.put(new Long(totalsales), new Text(myValue));
			
		

	      }
   
			protected void cleanup(Context context) throws IOException,
			InterruptedException 
			{
				double mypercent=0.00;
				long totalsales=0;
				String mytext="";
				String mykey="";
			
				for (Text t : repToRecordMap.values()) 
				{
						String token[]=t.toString().split(",");
						mykey=token[0];
						totalsales=Long.parseLong(token[1]);
						mypercent=totalsales/grand_total;
						
						String salespercent=String.format("%f",mypercent);
						
						 mytext=token[1]+""+salespercent;
						 
						 context.write(new Text(mykey), new Text(mytext));
						
						
				}
			}
	      
	   }
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
	
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Top Buyer");
	    job.setJarByClass(DaySale.class);
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
