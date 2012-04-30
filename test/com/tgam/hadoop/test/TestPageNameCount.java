package com.tgam.hadoop.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.tgam.hadoop.mapreduce.OmnitureDataFileInputFormat;

/**
 * A MapReduce job which tests using the org.tgam.hadoop.mapreduce.OmnitureDataFileInputFormat.
 * Output will be in the format:
 * <table>
 *   <tr>
 *     <th>Page Name</th><th>Hit Count (not in any order)</th>
 *   </tr>
 *   <tr>
 *     <td>About Us</td><td>1</td>
 *   </tr>
 *   <tr>
 *     <td>Add Product To Cart</td><td>8</td>
 *   </tr>
 *   <tr>
 *     <td>...</td><td>...</td>
 *   </tr>
 * </table><br>
 * To run this from the command line do the following:
 * <code>
 * hadoop jar YourGeneratedJarName.jar com.tgam.hadoop.test.TestPageNameCount <PATH TO SAMPLE HIT_DATA.TSV> <PATH TO OUTPUT DIRECTORY>
 * </code> 
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 *
 */
public class TestPageNameCount {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		private Text outputKey;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] fields = line.split("\t", -1);
						
			outputKey = new Text(fields[14]);			
			
			context.write(outputKey, ONE);
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
			int total = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				total += iterator.next().get();
			}
			
			context.write(key, new IntWritable(total));
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(OmnitureDataFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
