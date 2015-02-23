package com.refactorlabs.cs378.assign4;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.refactorlabs.cs378.assign4.WordStatistics.MapClass;
import com.refactorlabs.cs378.assign4.WordStatistics.ReduceClass;

/**
 * 
 * This is an extension of the word statistics class that we wrote 
 * in the second homework. We want to solve a common big data processing 
 * scenario - We run a data/statistics collection program at some 
 * regular interval (hourly, daily, weekly), and we want to aggregate 
 * those statistics for longer time periods (daily, weekly, monthly) 
 * without reprocessing the original data (email, in our case). 
 * 
 * @author gnanda
 *
 */
public class WordStatistics {

	/**
	 * Mapper Class.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {
		/**
		 * Counter group for the mapper. Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
		
		/**
		 * Create a private HashMap<String, Count>, so that we don't have to
		 * create the hash object on every map() invocation. However ensure
		 * to clear the HashMap before using during every invocation.
		 */
		private static final HashMap<String, Long> wordCountMap = 
				new HashMap<String, Long>();
		
		/**
		 * Create a word statistics object and re-use it.
		 */
		private static final WordStatisticsWritable wordStatisticsWritable =
				new WordStatisticsWritable();
			
		@Override
		public void map(LongWritable mapKey, Text value, Context context)
			throws IOException, InterruptedException {
			
			// Read in the lines
			String line = value.toString();
			
			// Tokenize the line.
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			// Increment the counter for the input lines mapper.
			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			// Clear the map for the consecutive invocations.
			wordCountMap.clear();
			
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				
				// Update the word count map.
				if (wordCountMap.containsKey(word)) {
					wordCountMap.put(word, wordCountMap.get(word) + 1);
				} else {
					wordCountMap.put(word, 1L);
				}
			}
			
			// Iterate over the map and update the word statistics object.
			for (String key : wordCountMap.keySet()) {
				long count = wordCountMap.get(key);
				long squareCount = count * count;
				
				wordStatisticsWritable.updateCounts(1L, count, squareCount);
				context.write(new Text(key), wordStatisticsWritable);
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}	
	}
	
	/**
	 * Reducer class that can also function as the combiner.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {
		/**
		 * Counter group for the reducer. Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Count";
		
		/**
		 * Create a word statistics object and re-use it.
		 */
		private static final WordStatisticsWritable wordStatisticsWritable =
				new WordStatisticsWritable();
		
		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
			throws IOException, InterruptedException {
			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
			
			wordStatisticsWritable.reset();
			
			for (WordStatisticsWritable value : values) {
				wordStatisticsWritable.addWordStatisticsWritable(value);
			}
			
			context.write(key, wordStatisticsWritable);
		}
	}
	
	
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    	Job job = new Job(conf, "WordStatistics");
    	
    	// Identify the JAR file to replicate to all machines.
    	job.setJarByClass(WordStatistics.class);
    	
    	// Set the output key and value types (for map and reduce).
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(WordStatisticsWritable.class);
    	
    	// Set the map, combiner and reduce classes.
    	job.setMapperClass(MapClass.class);
    	job.setCombinerClass(ReduceClass.class);
    	job.setReducerClass(ReduceClass.class);
    	
    	// Set the input and output file formats.
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	// Grab the input file and output directory from the command line.
    	FileInputFormat.addInputPath(job, new Path(appArgs[0]));
    	FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
    	
    	// Initiate the map-reduce job, and wait for completion.
    	job.waitForCompletion(true);
    }
}
