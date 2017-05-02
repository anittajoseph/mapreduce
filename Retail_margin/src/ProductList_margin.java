

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


	public class ProductList_margin {
		public static class productMapper extends Mapper<LongWritable, Text,Text, Text>
		{
			private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
			{
	try{
				String[] record = value.toString().split(";");
			    String pid = record[5];
			    String cp = record[7];
			    String sp = record[8];
			    String qty = record[6];
			    String myRow= cp+";"+sp+";"+qty;
	            context.write(new Text(pid), new Text(myRow));
	}
	catch(Exception e)
		{
		
		System.out.println(e);
		}
	
		}
		}
	
		public static class ProductReducer extends Reducer<Text, Text, NullWritable, Text> {
			private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

			public void reduce(Text key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
					
				long totalsales=0;
				long totalcost=0;
				int totalqty=0;
				long profit =0;
				double margin=0.00;
				
				for (Text value : values) 
				{
					String[] record = value.toString().split(";");
					totalsales=totalsales+Long.parseLong(record[1]);
					totalcost=totalcost+Long.parseLong(record[0]);
					totalqty=totalqty+Integer.parseInt(record[2]);
				}
				
				profit =totalsales-totalcost;
				if(totalcost!=0)
				{
				margin=(totalsales-totalcost)*100/totalcost;
			
				}
				else
				{
					margin=(totalsales-totalcost)*100;
					
				}
				 String myValue = key.toString();
		         String myprofit = String.format("%d", profit);
		         String mymargin = String.format("%f", margin);
		         String myqty = String.format("%d", totalqty);
		      myValue= myValue+','+myqty+','+myprofit+','+mymargin;
		      repToRecordMap.put(new Double(margin),new Text(myValue));
					
		}
			
		
		protected void cleanup(Context context) throws IOException,
		InterruptedException {

		for (Text t : repToRecordMap.descendingMap().values()) 
		{
		context.write(NullWritable.get(), t);
			}
		}
		}
		
		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Product List");
		    job.setJarByClass(ProductList_margin.class);
		    job.setMapperClass(productMapper.class);
		    job.setReducerClass(ProductReducer.class);
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	

		
	}
