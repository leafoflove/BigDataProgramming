package com.refactorlabs.cs378.assign2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

/**
 * Word statistics class is used to compute the mean and variance for each word that appears in one or more paragraphs of the input document.
 * We use Map-Reduce Paradigm to solve this problem.
 * 
 * @author Gaurav Nanda (nanda@utexas.edu)
 */

public class WordStatistics {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongArrayWritable> {
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
		 * Create one LongWritable object that could be reused.
		 */
		private static final LongArrayWritable longArrayWritable =
				new LongArrayWritable();
		
		/**
		 * These punctuation characters could not be a part of normal string tokenizer
		 * as that would also split words like good-natured. However, we would have
		 * to handle cases like "---good--". Similarly for "\'" also.
		 */
		private List<Character> punctuations = Arrays.asList('-', '\'');
		
		@Override
		public void map(LongWritable mapKey, Text value, Context context)
			throws IOException, InterruptedException {
			
			// Read in the line/paragraph.
			String line = value.toString();
			
			// Tokenize the paragraph.
			StringTokenizer tokenizer = new StringTokenizer(line, "=_\";:.,?[! ");
			
			// Count the number of paragraphs.
			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			// Clear the map for the consecutive invocations.
			wordCountMap.clear();
			
			// Read in each token and build a Hash of the word and its count
			// in the respective paragraph.
			while(tokenizer.hasMoreTokens()) {
				StringBuilder token = new StringBuilder(tokenizer.nextToken().toLowerCase());
				
				// Some code to strip off the words. This code would take
				// care of stripping of punctuation from front and behind.
				boolean anyChange = true;
				while(token.length() > 0 && anyChange) {
					anyChange = false;
					
					if (punctuations.contains(token.charAt(0))) {
						token.deleteCharAt(0);
						anyChange = true;
					}

					if (token.length() > 0 && punctuations.contains(token.charAt(token.length() - 1))) {
						token.deleteCharAt(token.length() - 1);
						anyChange = true;
					}
				}
				
				// If we ended up removing everything, then simply continue.
				if (token.length() == 0) {
					continue;
				}

				String word = token.toString();
								
				// This is to handle another weird case of "--", that comes between
				// two words. Split that into two words and update the map for both.
				if (word.contains("--")) {
					String word1 = word.substring(0, word.indexOf("--"));
					String word2 = word.substring(word.indexOf("--") + 2);
					updateWordCountMap(word1);
					updateWordCountMap(word1);
				} else {
					updateWordCountMap(word);

				}
			}
			
			// Iterate over the key in the HashMap and write the Long Array
			// having 1L, count of the word and respective square of the values.
			for (String key : wordCountMap.keySet()) {
				long count = wordCountMap.get(key);
				long squareCount = count * count;
				
				longArrayWritable.setValueArray(new long[] {1L, count, squareCount});
				context.write(new Text(key), longArrayWritable);
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}
	
		/*
		 * Fill the word count map. If it is the first occurrence, 
		 * then set the count to be 1, otherwise  increment count by 1.
		 */
		private void updateWordCountMap(String word) {
			// This is an one odd case, where words like precise[173]. 
			// According to Professor, these two should be two different words.
			// To Handle this, I am using "[" as string tokenizer, and later checking
			// if the token has "]", if so, I add "[" back and retain entire [173].
			if (word.charAt(word.length() - 1) == ']') {
				word = "[" + word;
			}
				
			if (wordCountMap.containsKey(word)) {
				wordCountMap.put(word, wordCountMap.get(word) + 1);
			} else {
				wordCountMap.put(word, 1L);
			}	
		}
	}
	
	/**
	 * Combiner class that would combine <paragraph_count, sum, square_sum>.
	 */
	public static class CombinerClass extends Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {
		/**
		 * Counter group for the combiner.
		 */
		private static final String COMBINER_COUNTER_GROUP = "Combiner Counts";
		
		/**
		 * Create one LongWritable object that could be reused.
		 */
		private LongArrayWritable longArrayWritable = new LongArrayWritable();
		
		public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// Count the number of paragraphs.
			context.getCounter(COMBINER_COUNTER_GROUP, "Words in").increment(1L);
						
			long count = 0;
			long sum = 0;
			long squaredSum = 0;
			
			// Combiner would simply sum up paragraph count, word count and its respective square count, 
			// for the same key.
			for (LongArrayWritable value: values) {
				long[] arrayValues = value.getValueArray();
				count += arrayValues[0];
				sum += arrayValues[1];
				squaredSum += arrayValues[2];
			}
			
			longArrayWritable.setValueArray(new long[] {count, sum, squaredSum});
			context.write(key, longArrayWritable);
			context.getCounter(COMBINER_COUNTER_GROUP, "Words Out").increment(1L);
		}
	}
	
	/**
	 * Reducer class would calculate the mean and variance.
	 */
	public static class ReduceClass extends Reducer<Text, LongArrayWritable, Text, DoubleArrayWritable> {
		/**
		 * Counter group for the reducer. Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		@Override
		public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			long count = 0L;
			double mean = 0;
			double variance = 0;
			for (LongArrayWritable value : values) {
				long[] arrayValues = value.getValueArray();
				count += arrayValues[0];
				mean += arrayValues[1];
				variance += arrayValues[2];
			}
			mean /= count;
			variance /= count;
			variance -= (mean * mean);
			
			// Emit the double array for each word.
			DoubleArrayWritable doubleArrayWritable1 = new DoubleArrayWritable();
			doubleArrayWritable1.setValueArray(new double[] {count, mean, variance});
			//System.out.println(doubleArrayWritable1);
			context.write(key, doubleArrayWritable1);
		}
	}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "MultipleStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);
		
		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleArrayWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongArrayWritable.class);
		
		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(CombinerClass.class);
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