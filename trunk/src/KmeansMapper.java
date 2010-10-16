import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeansMapper extends Mapper<LongWritable, Text, Text, ArrayList<String>>{
    
    //private coord.TextArrayWritable();
    private Text word = new Text();
    //private FloatWritable coord1 = new FloatWritable();
    private Text coord1= new Text();  
    
        
    
    
    
    
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException, InterruptedException {
      
    	//setto arraylist coordinate    
    	
      ArrayList<String> coordinate= new ArrayList<String>();
      //divido in token la prima riga del file
      StringTokenizer itr = new StringTokenizer(value.toString());
      //setto il primo token come key (num point)
      String key_add=itr.nextElement().toString();
      //meansCluster.set(key_add);
      
      word.set(key_add);
      Text out= new Text();
      
      
        //coord1.set(itr.nextElement().toString());
      while (itr.hasMoreTokens()) {
          coordinate.add(itr.nextToken().toString());
          //out.set(out.toString() + " "+ itr.nextToken().toString());
          output.collect(new Text(word),new Text(itr.nextToken().toString()));
      }
      System.out.println("Key: "+ key_add + "Coord: "+ coordinate.toString());
       
     // output.collect(word, out);
      
          
    }
  }
