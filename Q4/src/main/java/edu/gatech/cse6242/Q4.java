package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class Q4 {

	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable negOne = new IntWritable(-1);
	
	public static class GraphMapperOne
	   extends Mapper<Object, Text, IntWritable, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException  {
			String line = value.toString(); 
			String[] temp = line.split("\t");
			String source = temp[0];
			String target = temp[1];
			context.write(new IntWritable(Integer.parseInt(source)), one);
			context.write(new IntWritable(Integer.parseInt(target)), negOne); 
	  }
	}

	public static class GraphReducerSum
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

	private final static IntWritable sumFinal = new IntWritable();
	public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
  	  int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      } 
      sumFinal.set(sum);
      context.write(key, sumFinal);
    }
  }

  public static class GraphMapperTwo
	   extends Mapper<Object, Text, IntWritable, IntWritable>{


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException  {
			String line = value.toString(); 
			String[] temp = line.split("\t");
			String source = temp[0];
			// String diff = temp[1];
			context.write(new IntWritable(Integer.parseInt(temp[1])), one);
			// context.write(new IntWritable(Integer.parseInt(target)), new IntWritable(Integer.parseInt(-1))); 
	  }
	}

  public static void main(String[] args) throws Exception {
    Configuration confMapper1 = new Configuration();
    Configuration confMapper2 = new Configuration();
    Job jobMapper1 = Job.getInstance(confMapper1, "Q4");

    /* TODO: Needs to be implemented */
    jobMapper1.setJarByClass(Q4.class);

    jobMapper1.setMapperClass(GraphMapperOne.class);
    jobMapper1.setReducerClass(GraphReducerSum.class);

    jobMapper1.setOutputKeyClass(IntWritable.class);
    jobMapper1.setOutputValueClass(IntWritable.class);
    String bow = args[1] + "_Dummy";
    FileInputFormat.addInputPath(jobMapper1, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobMapper1, new Path(bow));
    jobMapper1.waitForCompletion(true);

    
    Job jobMapper2 = Job.getInstance(confMapper2, "Q4");

    /* TODO: Needs to be implemented */
    jobMapper2.setJarByClass(Q4.class);

    jobMapper2.setMapperClass(GraphMapperTwo.class);
    jobMapper2.setReducerClass(GraphReducerSum.class);

    jobMapper2.setOutputKeyClass(IntWritable.class);
    jobMapper2.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(jobMapper2, new Path(bow));
    FileOutputFormat.setOutputPath(jobMapper2, new Path(args[1]));
    System.exit(jobMapper2.waitForCompletion(true) ? 0 : 1);
  }
}
