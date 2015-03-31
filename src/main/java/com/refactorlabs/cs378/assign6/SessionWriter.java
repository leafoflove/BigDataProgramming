package com.refactorlabs.cs378.assign6;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
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

import com.refactorlabs.cs378.assign5.WordStatistics;
import com.refactorlabs.cs378.assign5.WordStatisticsData;
import com.refactorlabs.cs378.assign5.WordStatistics.ReduceClass;
import com.refactorlabs.cs378.assign5.WordStatistics.WordCountMapper;
import com.refactorlabs.cs378.sessions.*;

/**
 * Class to write user_id-<list of session> objects
 * @author nanda
 *
 */
public class SessionWriter extends Configured implements Tool {
	
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
	
	/*
	 *  Mapper class to define map() function.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {
		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
		
		// Text word object which is re-used.
		private Text word = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] tokens = line.split("\t");

			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			Session.Builder sessionBuilder = Session.newBuilder();
			Event event = new Event();
			Class<?> c = event.getClass();
			for (int i=0; i < tokens.length; ++i) {
				if (i == 0) { // user id case. 
					sessionBuilder.setUserId(tokens[i]);
				} else if (i == 1) { // event type and subtype case.
					String[] split = tokens[i].split(" ");
					if (split[0].equals("contact") && split[1].equals("form")) {
						event.setEventType(EventType.CONTACT_FORM_STATUS);
						event.setEventSubtype(EventSubtype.valueOf(split[2].toUpperCase()));
					} else if (split[0].equals("visit_market_report_listing")){
						event.setEventType(EventType.VISIT);
						event.setEventSubtype(EventSubtype.MARKET_REPORT_LISTING);
					} else {
						event.setEventType(EventType.valueOf(split[0].toUpperCase()));
						String joined = StringUtils.join(Arrays.copyOfRange(split, 1, split.length), "_");
						event.setEventSubtype(EventSubtype.valueOf(joined.toUpperCase()));
					}
				} else if (i == tokens.length - 1) { // features case.
					String[] split = tokens[i].split(":");
					List<CharSequence> features = new ArrayList<CharSequence>(Arrays.asList(split));
					event.setFeatures(features);
				}
				else {
					try { // Use reflection.
						Field f = c.getField(headers.get(i));
						f.set(event, tokens[i]);
						System.out.println("Setting " + tokens[i] + " for " + headers.get(i));
					} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			sessionBuilder.setEvents(new ArrayList<Event>(Arrays.asList(event)));
			word.set(tokens[0]);
			context.write(word, new AvroValue(sessionBuilder.build()));
		}
	}
	
	/*
	 *  Mapper class to define reduce() function. Combine all the session objects and merge them in one.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {
		
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";
		
		@Override
		public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
				throws IOException, InterruptedException {
			
			Session.Builder sessionBuilder = Session.newBuilder();
			sessionBuilder.setUserId(key.toString());
			ArrayList<Event> events = new ArrayList<Event>();
			for (AvroValue<Session> value : values) {
				Session se = value.datum();
				events.add(se.events.get(0));
			}
			// Sort using custom comparator.
			Collections.sort(events, new Comparator<Event>() {
	            	@Override
	            	public int compare(Event o1, Event o2) {
	            		return o1.event_timestamp.toString().compareTo(o2.event_timestamp.toString());
	            	}
	        });
			sessionBuilder.setEvents(events);
			// Output only where number of events are only 50.
			//if (events.size() != 50) {
			//	return;
			//}
			context.write(new AvroKey(key.toString()), new AvroValue(sessionBuilder.build()));
			
			context.getCounter(REDUCER_COUNTER_GROUP, "OutPut Lines").increment(1L);
		}
	}
	
	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: SessionWriter <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "SessionWriter");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(SessionWriter.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		// Specify the Map
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		// Specify the Reduce
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		//job.setOutputKeyClass(Text.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

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
		int res = ToolRunner.run(new Configuration(), new SessionWriter(), args);
		System.exit(res);
	}
}
