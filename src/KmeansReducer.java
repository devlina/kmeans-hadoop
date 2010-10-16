import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KmeansReducer extends Reducer<Text,Text,Text,Text> {
	  
    private Text result = new Text();

    public void reduce(Text key, Text values,Context context) throws IOException, InterruptedException {
      //Text sum = new Text();
     // for (Text val : values) {
       // sum = val;
     // }
     // result.set(sum);
      context.write(key, values);
    }
  }