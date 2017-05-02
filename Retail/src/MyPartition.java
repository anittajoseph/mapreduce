import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartition extends Partitioner<Text,Text>
{


    @Override
    public int getPartition(Text key, Text value, int numReduceTasks)
    {
       String[] str = value.toString().split(",");
       String agegroup=str[0];
       
       if(agegroup.equals("A"))
       {
          return 0;
       }
       else if(agegroup.equals("B"))
       {
          return 1;
       }
       else if(agegroup.equals("C"))
       {
          return 2;
       }
       
       
       else if(agegroup.equals("D"))
       {
          return 3;
       }
       else if(agegroup.equals("E"))
       {
          return 4;
       }
       else if(agegroup.equals("F"))
       {
          return 5;
       }
       else if(agegroup.equals("G"))
       {
          return 6;
       }
       else if(agegroup.equals("H"))
       {
          return 7;
       }
       else if(agegroup.equals("I"))
       {
          return 8;
       }
       else if(agegroup.equals("J"))
       {
          return 9;
       }
       else 
       {
          return 10;
       }
       
       
       
    }

}
