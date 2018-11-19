package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



import java.util.*; 

import org.apache.hadoop.util.*; 

public class Q1 {

  public static class GraphMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{

    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();

  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException  {
		String line = value.toString(); 
		String[] temp = line.split("\t");
		String node = temp[0];
		String lasttoken = temp[2];
		context.write(new IntWritable(Integer.parseInt(node)), new IntWritable(Integer.parseInt(lasttoken))); 
      }
    }

  public static class GraphReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
  	  int maxWeight = Integer.MIN_VALUE;
      for (IntWritable val : values) {
        maxWeight = Math.max(maxWeight, val.get());
      } 
      IntWritable maxWeight1 = new IntWritable(maxWeight);
      context.write(key, maxWeight1);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */

    job.setJarByClass(Q1.class);
    job.setMapperClass(GraphMapper.class);
    // job.setCombinerClass(GraphReducer.class);
    job.setReducerClass(GraphReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);


    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}