package comp9313.proj1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;
import java.lang.*;

import comp9313.proj1.StringIntPair;

public class Project1 {

	public static class TermWeightMapper
		extends Mapper<Object, Text, StringIntPair, IntWritable>{

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), " ,"); 
			String date = itr.nextToken();
			CharSequence year_char_seq = date.subSequence(0, 4);
			String year = year_char_seq.toString();

			/* For all words in headline, write key-value pair with value 
			   converted to key, and two special values, using MIN and MAX integer values.  */
			while (itr.hasMoreElements()) {
				String word = itr.nextToken();
				// Key value pair with year included in key.
				StringIntPair output_key = new StringIntPair(word, Integer.parseInt(year));  
				
				// Special key - pass year as value, key is min integer value to be sorted first
				StringIntPair special_key = new StringIntPair(word, Integer.MIN_VALUE);
				IntWritable special_year_value = new IntWritable(Integer.parseInt(year));
				
				// Special key - key is max value to appear at end of sort
				StringIntPair stop_key = new StringIntPair(word, Integer.MAX_VALUE);
				
				context.write(output_key, one);				
				context.write(special_key, special_year_value);
				context.write(stop_key, one);
			}
		}
	}
  
	public static class TermWeightCombiner
		extends Reducer<StringIntPair,IntWritable,StringIntPair,IntWritable> {
	  
		@Override
		public void reduce(StringIntPair key, Iterable<IntWritable> values, Context context
				) throws IOException, InterruptedException {
			// If key is special key, do not combine
			if (key.getSecond() == Integer.MIN_VALUE) {
				for (IntWritable val : values) {
					context.write(key, val);
				}
			// Aggregate values by key
			} else {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				context.write(key, new IntWritable(sum));
			} 
		}
	}
  
	public static class TermWeightPatitioner extends Partitioner<StringIntPair, IntWritable>{
		@Override
		public int getPartition(StringIntPair pair, IntWritable arg1, int numPartitions) {
			// Partition only by word
			return (pair.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
 
	public static class TermWeightReducer
		extends Reducer<StringIntPair, IntWritable,Text,Text> {
	
		// Global values to be used across reducer calls
		int num_years = 0;
		String output_string = "";
	
		public void reduce(StringIntPair key, Iterable<IntWritable> values,
		                   Context context
		                   ) throws IOException, InterruptedException {
			// Get years from configuration
			Configuration conf = context.getConfiguration();
			String string_years = conf.get("string_years");
			Integer years = Integer.parseInt(string_years);
				
			/* If key is minimum value (special key), calculate number 
			   of years for word, utilising set class to remove duplicates. */
			if (key.getSecond() == Integer.MIN_VALUE) {
				num_years = 0;
				output_string = "";
				Set<Integer> set = new HashSet<Integer>();
				for (IntWritable val : values) {
					set.add(val.get());
				}
				num_years = set.size();
				
			/* If key has maximum value, all weights for the 
				word has been calculated, write output to context.*/
			} else if (key.getSecond() == Integer.MAX_VALUE) {
				output_string = output_string.substring(0,output_string.length() -1);
				
				Text output_value = new Text();
				output_value.set(output_string);
				Text output_key = new Text();
				output_key.set(key.getFirst());
				
				context.write(output_key, output_value);
				
			/* Otherwise calculate weight for the year and add 
				to output string */
			} else { 
				double TF = 0.0;
				for (IntWritable val : values) {
					TF += (double)val.get();
				}
				 
				double IDF = Math.log10(((double)years)/((double)num_years));
				Double weight = TF*IDF;
				output_string += key.getSecond().toString() + "," + weight.toString() + ";";
			}
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("string_years", args[2]);
	    conf.set("mapred.textoutputformat.separator", "\t");
	    
	    Job job = Job.getInstance(conf, "Term Weights");
	    job.setNumReduceTasks(Integer.parseInt(args[3]));
	    job.setJarByClass(Project1.class);
	    job.setMapperClass(TermWeightMapper.class);
	    job.setCombinerClass(TermWeightCombiner.class);
	    job.setReducerClass(TermWeightReducer.class);
	    job.setPartitionerClass(TermWeightPatitioner.class);
	    
	    job.setMapOutputKeyClass(StringIntPair.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}