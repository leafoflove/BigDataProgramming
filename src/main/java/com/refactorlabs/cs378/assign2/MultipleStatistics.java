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
 * Tasks to be done in this homework
 * 
 * 1) Check out the dataset2 and check the kind of punctuation present in the data.
 * 2) Write the skeleton for the mapper, combiner and then reducer.
 * 3) Also parallely write the tester classes.
 * 
 * @author Gaurav Nanda (nanda@utexas.edu)
 *
 */

public class MultipleStatistics {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongArrayWritable> {
		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
		
		/**
		 * Create a private HashMap<Text, Count>, so that we dont have to
		 * create the hash object on every map() invocation.
		 */
		private static final HashMap<Text, Long> wordCountMap = 
				new HashMap<Text, Long>();
		
		/**
		 * Create one LongWritable object that could be reused.
		 */
		private static final LongArrayWritable longArrayWritable =
				new LongArrayWritable();
		
		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();
		
		private List<Character> punctuations = Arrays.asList('-', '_', '\"', ',', ';', ':');
		
		@Override
		public void map(LongWritable mapKey, Text value, Context context)
			throws IOException, InterruptedException {
			
			// Read in the line/paragraph.
			String line = value.toString();
			
			// Tokenize the paragraph.
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			// Count the number of paragraphs.
			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			// Clear the map for the consecutive invocations.
			wordCountMap.clear();
			
			// Read in each token and build a Hash of the word and its count
			// in the respective paragraph.
			while(tokenizer.hasMoreTokens()) {
				StringBuilder token = new StringBuilder(tokenizer.nextToken().toLowerCase());
				
				// Some code to strip off the words.
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
				word.set(token.toString());
				
				// Fill the word count map. If it is the first 
				// occurrence, then set the count to be 1, otherwise
				// increment count by 1.
				if (wordCountMap.containsKey(word)) {
					wordCountMap.put(word, wordCountMap.get(word) + 1);
				} else {
					wordCountMap.put(word, 1L);
				}
			}
			
			for (Text key : wordCountMap.keySet()) {
				long count = wordCountMap.get(key);
				long squareCount = count * count;
				
				longArrayWritable.setValueArray(new long[] {1L, count, squareCount});
				context.write(key, longArrayWritable);
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}
	}
	
	/**
	 * Combiner class that would combine <paragraph_count, avg, square_avg>.
	 */
	public static class CombinerClass extends Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {
		/**
		 * Counter group for the combiner.
		 */
		private static final String COMBINER_COUNTER_GROUP = "Combiner Counts";
		
		/**
		 * Create one LongWritable object that could be reused.
		 */
		LongArrayWritable longArrayWritable = new LongArrayWritable();
		
		public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// Count the number of paragraphs.
			context.getCounter(COMBINER_COUNTER_GROUP, "Words in").increment(1L);
						
			long count = 0;
			long sum = 0;
			long squaredSum = 0;
			
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
			long mean = 0L;
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
			doubleArrayWritable1.setValueArray(new double[] {mean, variance});
			System.out.println(doubleArrayWritable1);
			context.write(key, doubleArrayWritable1);
		}
	}
		
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "MultipleStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(MultipleStatistics.class);
		
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