package com.refactorlabs.cs378.assign6;

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
import org.junit.internal.builders.IgnoredBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * WordCount app to output the following info:
 * fieldname:value count
 * 
 * This will show you the values that occur in each field including which fields have no value or a null value.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCount {

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
	public final static LongWritable ONE = new LongWritable(1L);
	
	/**
	 * List of all the headers.
	 */
	public final static ArrayList<String> headers = new ArrayList<String>(
				Arrays.asList("user_id", "event_type",	"page",	"referrer",	
				"referring_domain", "event_timestamp", "city", "region", "vin", "vehicle_condition",	
				"year",	"make",	"model", "trim", "body_style", "subtrim", "cab_style", "initial_price",	
				"mileage", "mpg", "exterior_color", "interior_color", "engine_displacement", "engine",
				"transmission",	"drive_type", "fuel", "image_count", "initial_carfax_free_report",
				"carfax_one_owner", "initial_cpo", "features"));
	
	/**
	 * Ignore Headers.
	 */
	public final static ArrayList<String> ignoredHeaders = new ArrayList<String>(
				Arrays.asList("event_timestamp", "image_count", "initial_price",
				"mileage", "referrer", "user_id", "vin"));

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\t");

			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			// For each word in the input line, emit a count of 1 for that word.
			for (int i=0; i < tokens.length; ++i) {
				String token = tokens[i];
				if (ignoredHeaders.contains(headers.get(i))) {
					continue;
				}
				
				word.set(headers.get(i) + ":" + token);
				context.write(word, ONE);
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;

			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (LongWritable value : values) {
				sum += value.get();
			}
			// Emit the total count for the word.
			context.write(key, new LongWritable(sum));
		}
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "WordCount");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordCount.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
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
