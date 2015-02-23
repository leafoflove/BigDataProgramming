package com.refactorlabs.cs378.assign4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.refactorlabs.cs378.assign4.WordStatistics.MapClass;
import com.refactorlabs.cs378.assign4.WordStatistics.ReduceClass;

/**
 * This class takes multiple files output by the new WordStatistics program and
 * aggregates counts for same word and compute new statistics.
 * 
 * @author gnanda
 *
 */
public class WordStatisticsAggregator {
	/**
	 * Mapper Class.
	 */
	public static class MapClass extends Mapper<Text, Text, Text, WordStatisticsWritable> {
		/**
		 * Counter group for the mapper. Individual counters are grouped for the mapper.
		 */
		private static final String AGGREGATOR_MAPPER_COUNTER_GROUP = "Aggregator Mapper Counts";
	
		@Override
		public void map(Text mapKey, Text value, Context context)
			throws IOException, InterruptedException {
			
			// Increment the counter for the input lines mapper.
			context.getCounter(AGGREGATOR_MAPPER_COUNTER_GROUP, "INPUT WORDS").increment(1L);
			
			WordStatisticsWritable wordStatisticsWritable = WordStatisticsWritable.fromString(value.toString());
			context.write(new Text(mapKey), wordStatisticsWritable);
		}
	}	

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    	Job job = new Job(conf, "WordStatisticsAggregator");
    	
    	// Identify the JAR file to replicate to all machines.
    	job.setJarByClass(WordStatisticsAggregator.class);
    	
    	// Set the output key and value types (for map and reduce).
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(WordStatisticsWritable.class);
    	
    	// Set the map, combiner and reduce classes.
    	job.setMapperClass(MapClass.class);
    	// Extra-Credit. Use same reducer class as the Word Statistics.
    	job.setCombinerClass(WordStatistics.ReduceClass.class);
    	job.setReducerClass(WordStatistics.ReduceClass.class);
    	
    	// Set the input and output file formats.
    	job.setInputFormatClass(KeyValueTextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	// Grab the input file and output directory from the command line.
    	FileInputFormat.addInputPaths(job, appArgs[0]);
    	FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
    	
    	// Initiate the map-reduce job, and wait for completion.
    	job.waitForCompletion(true);
    }	
}
