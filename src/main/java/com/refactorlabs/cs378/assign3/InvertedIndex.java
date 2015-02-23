package com.refactorlabs.cs378.assign3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * This class would create an inverted index, where the output is a file of key/value pairs, 
 * with the key being a word, and the value being a list of verses in which that word appears.
 * 
 * @author gnanda
 *
 */
public class InvertedIndex {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, VerseArrayWritable> {
		/**
		 * Counter group for the mapper. Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
		
		/**
		 * Create a Hashset that could be reused in the mapper for getting a list of 
		 * all unique words.
		 */
		private HashSet<String> uniqueWords = new HashSet<String>();
		
		@Override
		public void map(LongWritable mapKey, Text value, Context context)
			throws IOException, InterruptedException {
			
			// Read in a line.
			String line = value.toString().trim();
			
			// Ignore the empty line.
			if (line.equals("")) {
				return;
			}
			
			String verseString = line.substring(0, line.indexOf(" "));
			Verse verse = new Verse(verseString);
			VerseArrayWritable verseArrayWritable = new VerseArrayWritable(new Verse[] {verse});
			
			// Tokenize the paragraph.
			StringTokenizer tokenizer = new StringTokenizer(normalizeString(line.substring(line.indexOf(" ") + 1)));
			
			// Count the number of paragraphs.
			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			// Clear the hashset for multiple map() invocations.
			uniqueWords.clear();
			
			// Read in each token and create a hashset of all the words.
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				uniqueWords.add(token);
			}
			
			for (String word : uniqueWords) {
				context.write(new Text(word), verseArrayWritable);
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}
		
		
		private String normalizeString(String str) {
			// Here we make the strings to be lower case and remove extra characters from the input.
			str = str.toLowerCase();
			str = StringUtils.replaceEach(str, new String[] {"=", "_", "\"", ";", ":", ".", ",", "?", "[", "]", "(", ")", "!", "--"}, 
										new String[] {" ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " "});
			
			return str;
		}
	}
		
	/**
	 * Combiner class that would combine all the verses for the given word.
	 */
	public static class CombinerClass extends Reducer<Text, VerseArrayWritable, Text, VerseArrayWritable> {
		/**
		 * Counter group for the combiner.
		 */
		private static final String COMBINER_COUNTER_GROUP = "Combiner Counts";
				
		public void reduce(Text key, Iterable<VerseArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Verse> valueArrayList = new ArrayList<Verse>();
			
			for(VerseArrayWritable verseArrayWritable : values) {
				Writable[] verseArray = verseArrayWritable.get();
				for (Writable verse : verseArray) {
					valueArrayList.add((Verse)verse);
				}
			}
				
			VerseArrayWritable verseArrayWritable = new VerseArrayWritable(valueArrayList.toArray());
			context.write(key, verseArrayWritable);
			context.getCounter(COMBINER_COUNTER_GROUP, "Words Out").increment(1L);
		}

	}
	
	/**
	 * Reducer class that would combine all the verses for the given word.
	 */
	public static class ReduceClass extends Reducer<Text, VerseArrayWritable, Text, VerseArrayWritable> {
		/**
		 * Counter group for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";
				
		public void reduce(Text key, Iterable<VerseArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Verse> valueArrayList = new ArrayList<Verse>();
				
			for(VerseArrayWritable verseArrayWritable : values) {
				Writable[] verseArray = verseArrayWritable.get();
				for (Writable verse : verseArray) {
					valueArrayList.add((Verse)verse);
				}
			}
				
			VerseArrayWritable verseArrayWritable = new VerseArrayWritable(valueArrayList.toArray());
			context.write(key, verseArrayWritable);
			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
		}

	}
	
	// Main function.
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);
		
		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VerseArrayWritable.class);
		
		// Set the map, combiner and reduce classes.
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