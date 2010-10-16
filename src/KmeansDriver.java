import java.io.ByteArrayOutputStream;

import java.io.IOException;

import java.io.PrintStream;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Float;


import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.jasper.tagplugins.jstl.core.Set;

public class KmeansDriver {
	
	private static HashMap<String,ArrayList<Float>> meansCluster= new HashMap<String,ArrayList<Float>>();
    
	
	public static void configure() {
        try {
                
                
                
        	

            
             ByteArrayOutputStream byte1=new ByteArrayOutputStream();
             PrintStream out2 = new PrintStream(byte1);
             
             
                String uri = "/user/unicondor/input/cluster";
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                fs.setDefaultUri(conf, uri);
                FSDataInputStream in = null;
                try {
                in = fs.open(new Path(uri));
                IOUtils.copyBytes(in, out2, 4096, false);   
                String s=byte1.toString();
                
                
               
                	String lines[]= s.split("\n");
                	
                	for(int i=0; i<lines.length; i++){
                	
                    ArrayList<Float> centers = new ArrayList<Float>();
                    
                    StringTokenizer itr = new StringTokenizer(lines[i]);
                    String id_cluster= itr.nextElement().toString();
                    
                    while(itr.hasMoreElements()){
                    	centers.add(Float.parseFloat(itr.nextElement().toString()));
                    }
                   meansCluster.put(id_cluster, centers);
                   
                	}
                   
                	//DEBUG
                	Iterator iter=meansCluster.keySet().iterator();
                	
                	while (iter.hasNext()) {  
                		   String key = iter.next().toString();  
                		   String value = meansCluster.get(key).toString();  
                		   
                		   System.out.println(key + " " + value);  
                		   System.out.println();
                		}  
                	
                	
                   //System.out.println("DEBUG "+meansCluster.toString());

            
                } 
                finally {
                IOUtils.closeStream(in);
                }
                
                
                
               
               
              
               
                
        } catch (IOException e) {
                e.printStackTrace();
        }
}
	

 
    
    
    
  
 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: kmeans <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf,"Kmeans");
    //mio
    job.setJobName("Kmeans_Unicondor");
    job.setJarByClass(KmeansDriver.class);
    job.setMapperClass(KmeansMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(KmeansReducer.class);
  //  job.setInputFormatClass(TextInputFormat.class);
    //job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    configure();
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



