import java.io.IOException;
import java.text.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//use retail data D11,D12,D01 and D02

public class AvgCost {

	// Mapper Class	
	
	   public static class AvgCostMapperClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            String prodid = str[5];
	            String cost = str[7];
	            String qty = str[6];
	            String myValue = qty + ',' + cost;
	            context.write(new Text(prodid), new Text(myValue));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }


	   //Reducer class
		
	   public static class AvgCostReducerClass extends Reducer<Text,Text,Text,Text>
	   {
		

	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	         long totalcost = 0;
	         int totalqty = 0;;
	         double avgcost = 0.00;
	         DecimalFormat df = new DecimalFormat("###.##");
	         for (Text val : values)
	         {
		         String[] token = val.toString().split(",");
	        	 totalqty = totalqty + Integer.parseInt(token[0]);
		         totalcost = totalcost + Long.parseLong(token[1]);
	        	 
	         }
	         		
	        avgcost = ((double)totalcost/(double)totalqty);
	        
	     
	        String myAvgCost = df.format(avgcost);
	        String myTotalCost = String.format("%d", totalcost);
	        String myTotalQty = String.format("%d", totalqty);	        
	        String myValue = myTotalQty + ',' + myTotalCost+ ',' + myAvgCost ;
			
	        context.write(key, new Text(myValue));
	       
	      }
      
	
	      
	   }

//Main class
	   
	   public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
		    conf.set("mapreduce.output.textoutputformat.separator",",");
			Job job = new Job(conf, "Top 5 products Sold Age wise");
			job.setJarByClass(AvgCost.class);
		    job.setMapperClass(AvgCostMapperClass.class);
		    job.setReducerClass(AvgCostReducerClass.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
