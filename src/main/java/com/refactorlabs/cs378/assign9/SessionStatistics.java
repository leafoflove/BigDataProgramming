package com.refactorlabs.cs378.assign9;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import com.google.common.collect.Maps;
import com.refactorlabs.cs378.sessions.*;

/**
 * Session Statistics is there for job chaining. We would compute statistics for 
 * Submitter, Sharer and Clicker.
 * 
 * Sample Execution Step on AWS:
 * 
 * com.refactorlabs.cs378.assign9.SessionStatistics 
 * s3n://utcs378/gn3522/output/assign8_run5/Submitter-m-00000.avro,s3n://utcs378/gn3522/output/assign8_run5/Submitter-m-00001.avro,s3n://utcs378/gn3522/output/assign8_run5/Submitter-m-00002.avro 
 * s3n://utcs378/gn3522/output/assign9_run7a 
 * s3n://utcs378/gn3522/output/assign8_run5/Sharer-m-00000.avro,s3n://utcs378/gn3522/output/assign8_run5/Sharer-m-00001.avro,s3n://utcs378/gn3522/output/assign8_run5/Sharer-m-00002.avro 
 * s3n://utcs378/gn3522/output/assign9_run7b 
 * s3n://utcs378/gn3522/output/assign8_run5/Clicker-m-00000.avro,s3n://utcs378/gn3522/output/assign8_run5/Clicker-m-00001.avro,s3n://utcs378/gn3522/output/assign8_run5/Clicker-m-00002.avro 
 * s3n://utcs378/gn3522/output/assign9_run7c 
 * s3n://utcs378/gn3522/output/assign9_run7a/part-r-00000.avro,s3n://utcs378/gn3522/output/assign9_run7b/part-r-00000.avro,s3n://utcs378/gn3522/output/assign9_run7c/part-r-00000.avro 
 * s3n://utcs378/gn3522/output/assign9_run7_FINAL
 * 
 * @author gnanda
 *
 */
public class SessionStatistics extends Configured implements Tool {
	/*
	 *  Base mapper class to read the AVRO container file with your sessions. 
	 */
	public static abstract class BaseMapper
		extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {
		
		// This is abstract function to be overridden by the child classes.
		public abstract String getSessionType();
		
		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
			Session session = value.datum();
			
			Map<EventSubtype, Integer> eventSubTypeCountMap = Maps.newHashMap();
			
			// Go over all the events and find out the unique counts.
			for (Event event : session.getEvents()) {
				if (event.getEventType() == EventType.CLICK) {
					Integer count = eventSubTypeCountMap.get(event.getEventSubtype());
							
					if (count == null) {
						eventSubTypeCountMap.put(event.getEventSubtype(), 1);
					} else {
						eventSubTypeCountMap.put(event.getEventSubtype(), count.intValue() + 1);
					}
				}
			}
			
			// Iterate over all the event subtypes and write into the context.
			for (EventSubtype subType : EventSubtype.values()) {
				ClickSubtypeStatisticsKey.Builder keyBuilder = ClickSubtypeStatisticsKey.newBuilder();
				keyBuilder.setSessionType(getSessionType());
				keyBuilder.setClickSubtype(subType.name());
				
				ClickSubtypeStatisticsData.Builder dataBuilder = ClickSubtypeStatisticsData.newBuilder();
				// Set session count.
				dataBuilder.setSessionCount(1);
				
				// Set total count.
				Integer count = eventSubTypeCountMap.get(subType);
				int total_count = 0;
				if (count != null) {
					total_count = count.intValue();
				} 					
				dataBuilder.setTotalCount(total_count);
				dataBuilder.setSumOfSquares(total_count * total_count);
				
				context.write(new AvroKey(keyBuilder.build()), new AvroValue(dataBuilder.build()));
			}	
		}
	}
	
	// Overridden Submitter, Sharer and Clicker classes.
	public static class SubmitterMapper extends BaseMapper {
		public String getSessionType() {
			return "Submitter";
		}
	}
	
	public static class SharerMapper extends BaseMapper {
		public String getSessionType() {
			return "Sharer";
		}
	}
	
	public static class ClickerMapper extends BaseMapper {
		public String getSessionType() {
			return "Clicker";
		}
	}
	
	/**
	 * Reducer class which would do the left join. This reducer can be used across all different mappers.
	 *
	 */
	public static class ReduceClass 
		extends Reducer<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>, 
						AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {
		
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";
		
		@Override
		public void reduce(AvroKey<ClickSubtypeStatisticsKey> key, Iterable<AvroValue<ClickSubtypeStatisticsData>> values, Context context)
				throws IOException, InterruptedException {
			long sessionCount = 0L;
			long totalCount = 0L;
			long sumOfSquares = 0L;
			
			ClickSubtypeStatisticsKey keyDatum = key.datum();
			
			HashSet<Long> uniqueValues = new HashSet<Long>();
			for (AvroValue<ClickSubtypeStatisticsData> value : values) {
				sessionCount += value.datum().getSessionCount();
				totalCount += value.datum().getTotalCount();
				sumOfSquares += value.datum().getSumOfSquares();
				if (keyDatum.getClickSubtype().equals("any")) {
					uniqueValues.add(value.datum().getSessionCount());
				}
			}
			if (!keyDatum.getSessionType().equals("any") && keyDatum.getClickSubtype().equals("any")) {
				sessionCount = uniqueValues.iterator().next();
			}
			
			if (keyDatum.getSessionType().equals("any") && keyDatum.getClickSubtype().equals("any")) {
				sessionCount = 0;
				for (Long value : uniqueValues) {
					sessionCount += value;
				}
 			}
			
			ClickSubtypeStatisticsData.Builder builder = ClickSubtypeStatisticsData.newBuilder();
			builder.setSessionCount(sessionCount);
			builder.setTotalCount(totalCount);
			builder.setSumOfSquares(sumOfSquares);
			
			double mean = (double)totalCount / sessionCount;
			builder.setMean(mean);
			builder.setVariance((double)sumOfSquares / sessionCount - mean * mean);
			
			context.write(key, new AvroValue<ClickSubtypeStatisticsData>(builder.build()));
			context.getCounter(REDUCER_COUNTER_GROUP, "Input Words").increment(1L);
		}	
	}
	
	/**
	 * Identity Mapper that takes three files as input and write them out 
	 * along with aggregated statistics.
	 */
	public static class IdentityMapper
		extends Mapper<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>, 
					   AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {
		
		@Override
		public void map(AvroKey<ClickSubtypeStatisticsKey> key, AvroValue<ClickSubtypeStatisticsData> value, Context context)
				throws IOException, InterruptedException {
			// Write the same key value pair.
			// 1) session_type, clicksubtype.
			context.write(key, value);
			
			ClickSubtypeStatisticsKey actualKey = key.datum();
			
			// Also write the same values with any as new key.
			// 2) any - clicksubtype.
			ClickSubtypeStatisticsKey.Builder keyBuilder = ClickSubtypeStatisticsKey.newBuilder();
			keyBuilder.setSessionType("any");
			keyBuilder.setClickSubtype(actualKey.getClickSubtype());
			context.write(new AvroKey(keyBuilder.build()), value);
		
			// Extra-Credit.
			// 1) any-any
			keyBuilder = ClickSubtypeStatisticsKey.newBuilder();
			keyBuilder.setSessionType("any");
			keyBuilder.setClickSubtype("any");
			context.write(new AvroKey(keyBuilder.build()), value);
			
			// 2) session_type, any
			keyBuilder = ClickSubtypeStatisticsKey.newBuilder();
			keyBuilder.setSessionType(actualKey.getSessionType());
			keyBuilder.setClickSubtype("any");
			context.write(new AvroKey(keyBuilder.build()), value);
		}
	}

	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 8) {
			System.err.println("Usage: SessionStatistics <input path1> <output path1> <input path2> <output path2> <input path3> <output path3>");
			return -1;
		}

		Configuration conf = getConf();
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		////////////////////////////////////////////////////////////////////////////////
		
		Job job = new Job(conf, "SubmitterMapper");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(SessionStatistics.class);

		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());

		// Mapper.
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		job.setMapperClass(SubmitterMapper.class);
		AvroJob.setMapOutputKeySchema(job, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(job, ClickSubtypeStatisticsData.getClassSchema());		
		
		// Specify the Reduce
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		AvroJob.setOutputKeySchema(job, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(job, ClickSubtypeStatisticsData.getClassSchema());

		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
		
		//////////////////////////////////////////////////////////////////////
		
		Job job1 = new Job(conf, "SharerMapper");

		// Identify the JAR file to replicate to all machines.
		job1.setJarByClass(SessionStatistics.class);

		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job1, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job1, Session.getClassSchema());

		// Mapper.
		job1.setInputFormatClass(AvroKeyValueInputFormat.class);
		job1.setMapperClass(SharerMapper.class);
		AvroJob.setMapOutputKeySchema(job1, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(job1, ClickSubtypeStatisticsData.getClassSchema());		
		
		// Specify the Reduce
		job1.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job1.setReducerClass(ReduceClass.class);
		AvroJob.setOutputKeySchema(job1, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(job1, ClickSubtypeStatisticsData.getClassSchema());

		// Grab the input file and output directory from the command line.
		inputPaths = appArgs[2].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job1, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job1, new Path(appArgs[3]));

		//////////////////////////////////////////////////////////////////////
		
		Job job2 = new Job(conf, "ClickerMapper");

		// Identify the JAR file to replicate to all machines.
		job2.setJarByClass(SessionStatistics.class);

		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job2, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job2, Session.getClassSchema());

		// Mapper.
		job2.setInputFormatClass(AvroKeyValueInputFormat.class);
		job2.setMapperClass(ClickerMapper.class);
		AvroJob.setMapOutputKeySchema(job2, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(job2, ClickSubtypeStatisticsData.getClassSchema());		
		
		// Specify the Reduce
		job2.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job2.setReducerClass(ReduceClass.class);
		AvroJob.setOutputKeySchema(job2, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(job2, ClickSubtypeStatisticsData.getClassSchema());

		// Grab the input file and output directory from the command line.
		inputPaths = appArgs[4].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job2, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job2, new Path(appArgs[5]));

		// Initiate the map-reduce jobs, and wait for completion.
		job.submit();
		job1.submit();
		job2.submit();

		while(! (job.isComplete() && job1.isComplete() && job2.isComplete()) ) {
			Thread.sleep(2000);
		}
		
		//////////////////////////////////////////////////////////////////////
		Job job3 = new Job(conf, "IdentityAggregator");

		// Identify the JAR file to replicate to all machines.
		job3.setJarByClass(SessionStatistics.class);

		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job3, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setInputValueSchema(job3, ClickSubtypeStatisticsData.getClassSchema());

		// Mapper.
		job3.setInputFormatClass(AvroKeyValueInputFormat.class);
		job3.setMapperClass(IdentityMapper.class);
		AvroJob.setMapOutputKeySchema(job3, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(job3, ClickSubtypeStatisticsData.getClassSchema());		
		
		// Specify the Reduce
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setReducerClass(ReduceClass.class);
		AvroJob.setOutputKeySchema(job3, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(job3, ClickSubtypeStatisticsData.getClassSchema());

		// Grab the input file and output directory from the command line.
		inputPaths = appArgs[6].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job3, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job3, new Path(appArgs[7]));

		job3.waitForCompletion(true);
		return 0;
	}
	
	private static void printClassPath() {
		ClassLoader cl = ClassLoader.getSystemClassLoader();
		URL[] urls = ((URLClassLoader) cl).getURLs();
		System.out.println("classpath BEGIN");
		for (URL url : urls) {
			System.out.println(url.getFile());
		}
		System.out.println("classpath END");
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		printClassPath();
		int res = ToolRunner.run(new Configuration(), new SessionStatistics(), args);
		System.exit(res);
	}
}