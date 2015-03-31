package com.refactorlabs.cs378.assign7;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

import com.refactorlabs.cs378.assign6.SessionWriter;
import com.refactorlabs.cs378.assign6.SessionWriter.MapClass;
import com.refactorlabs.cs378.assign6.SessionWriter.ReduceClass;
import com.refactorlabs.cs378.sessions.*;

/**
 * This class would be used to join two different data sources using a reduce-side join.
 * 
 * @author gnanda
 *
 */
public class MultipleInputLeftJoin extends Configured implements Tool {
	/*
	 *  Mapper class to read the AVRO container file with your sessions. 
	 */
	public static class SessionFileMapper 
		extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {
	
		// Text word object which is re-used.
		private Text word = new Text();
		
		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
			String userId = key.datum().toString();
			
			Session session = value.datum();
			
			// These maps would store Vin's vs various stats for the user in context.
			HashMap<String, Long> vinUniqueUsers = new HashMap<>();
			HashMap<String, Long> vinShareMarketReport = new HashMap<>();
			HashMap<String, Long> vinSubmitContactForm = new HashMap<>();
			HashMap<String, Map<CharSequence, Long>> vinClicks = new HashMap<>();
			
			// Go over all the events and find out the unique counts.
			for (Event event : session.getEvents()) {
				String vin = event.getVin().toString();
				vinUniqueUsers.put(vin, 1L);
				
				if (event.getEventType() == EventType.SHARE && event.getEventSubtype() == EventSubtype.MARKET_REPORT) {
					vinShareMarketReport.put(vin, 1L);
				}
				
				if (event.getEventType() == EventType.SUBMIT && event.getEventSubtype() == EventSubtype.CONTACT_FORM) {
					vinSubmitContactForm.put(vin, 1L);
				}
				
				if (event.getEventType() == EventType.CLICK) {
					if (vinClicks.containsKey(vin)) {
						Map<CharSequence, Long> clickMap = vinClicks.get(vin);
						clickMap.put(event.getEventSubtype().name(), 1L);
					} else {
						Map<CharSequence, Long> map = new HashMap<CharSequence, Long>();
						map.put(event.getEventSubtype().name(), 1L);
						vinClicks.put(vin, map);
					}
				}
			}
			
			// Once we have filled out the maps, we need to create builders for all the vins for 
			// the specific user.
			for (String vin : vinUniqueUsers.keySet()) {
				VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
				vinBuilder.setUniqueUser(1L);
				if (vinShareMarketReport.containsKey(vin)) {
					vinBuilder.setShareMarketReport(1L);
				}
				if (vinSubmitContactForm.containsKey(vin)) {
					vinBuilder.setSubmitContactForm(1L);
				}
				
				if (vinClicks.containsKey(vin)) {
					vinBuilder.setClicks(vinClicks.get(vin));
				}
				
				word.set(vin);
				context.write(word, new AvroValue(vinBuilder.build()));
			}
		}
	}
	
	/** 
	 * Mapper class to read the CSV file and populate the unique_user_vdp_view.
	 * 
	 */
	public static class VinVDPMapper 
		extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

		// Text word object which is re-used.
		private Text word = new Text();
	
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			// This one simply counts the impression count.
			String[] split = value.toString().split(",");
			
			if (split[2].equals("count")) {
				return;
			}
			
			VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
			vinBuilder.setUniqueUserVdpView(Long.parseLong(split[2]));
			
			word.set(split[0]);
			context.write(word, new AvroValue<>(vinBuilder.build()));
		} 
	}
	
	/**
	 * Combiner class which would do simply join output from mappers, but not filter.
	 *
	 */
	public static class CombinerClass 
		extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {
		// Text word object which is re-used.
		private Text word = new Text();
		
		public static VinImpressionCounts.Builder combineImpressionCounts(Text key, Iterable<AvroValue<VinImpressionCounts>> values) 
				throws IOException, InterruptedException {
			
			VinImpressionCounts.Builder sessionBuilder = VinImpressionCounts.newBuilder();
			
			// Map to hold the clicks map.
			Map<CharSequence, Long> map = new HashMap<CharSequence, Long>();
			
			// Iterate over various impression count objects for the given Vin.
			for (AvroValue<VinImpressionCounts> value : values) {
				VinImpressionCounts obj = value.datum();
				if (obj.getUniqueUserVdpView() != 0) {
					sessionBuilder.setUniqueUserVdpView(obj.getUniqueUserVdpView());
				} else {
					sessionBuilder.setUniqueUser(sessionBuilder.getUniqueUser() + obj.getUniqueUser());
					sessionBuilder.setShareMarketReport(sessionBuilder.getShareMarketReport() + obj.getShareMarketReport());
					sessionBuilder.setSubmitContactForm(sessionBuilder.getSubmitContactForm() + obj.getSubmitContactForm());
					Map<CharSequence, Long> clickMap = obj.getClicks();
					if (clickMap != null) {
						for (Map.Entry<CharSequence, Long> entry : clickMap.entrySet()) {
							String entryKey = entry.getKey().toString();
							if (map.containsKey(entryKey)) {
								map.put(entryKey, map.get(entryKey) + entry.getValue());
							} else {
								map.put(entryKey, entry.getValue());
							}
						}
					}
				}
			}
			sessionBuilder.setClicks(map);
			return sessionBuilder;
		}
		
		@Override
		public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
				throws IOException, InterruptedException {
			
			VinImpressionCounts.Builder sessionBuilder = combineImpressionCounts(key, values);
			context.write(key, new AvroValue(sessionBuilder.build()));
		}
	}	
	
	/**
	 * Reducer class which would do the left join.
	 *
	 */
	public static class ReduceClass 
		extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {
		// Text word object which is re-used.
		private Text word = new Text();
		
		@Override
		public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
				throws IOException, InterruptedException {
			
			// We are re-using the code from the combiner class directly.
			VinImpressionCounts.Builder sessionBuilder = CombinerClass.combineImpressionCounts(key, values);
			if (sessionBuilder.getUniqueUser() != 0) {
				context.write(key, new AvroValue(sessionBuilder.build()));
			}	
		}
	}

	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: MultipleInputLeftJoin <input path1> <input path2> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "MultipleInputLeftJoin");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(MultipleInputLeftJoin.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		// Specify the multiple inputs for mappers.
		MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionFileMapper.class);
		MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, VinVDPMapper.class);
		
		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());

		// Set other mapper related settings.
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

		// Set combiner class.
		job.setCombinerClass(CombinerClass.class);
		
		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

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
		int res = ToolRunner.run(new Configuration(), new MultipleInputLeftJoin(), args);
		System.exit(res);
	}
}