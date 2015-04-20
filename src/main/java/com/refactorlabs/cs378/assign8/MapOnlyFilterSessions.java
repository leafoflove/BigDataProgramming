package com.refactorlabs.cs378.assign8;

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
import org.apache.avro.mapreduce.AvroMultipleOutputs;
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

import com.refactorlabs.cs378.assign7.MultipleInputLeftJoin;
import com.refactorlabs.cs378.assign7.VinImpressionCounts;
import com.refactorlabs.cs378.assign7.MultipleInputLeftJoin.CombinerClass;
import com.refactorlabs.cs378.assign7.MultipleInputLeftJoin.ReduceClass;
import com.refactorlabs.cs378.assign7.MultipleInputLeftJoin.SessionFileMapper;
import com.refactorlabs.cs378.assign7.MultipleInputLeftJoin.VinVDPMapper;
import com.refactorlabs.cs378.sessions.*;

/**
 * This class would be used to perform maponly filtering on some session objects.
 * 
 * We would be using AvroContainer file as input file and AvroKeyValue pair as output.
 * 
 * @author gnanda
 *
 */
public class MapOnlyFilterSessions extends Configured implements Tool {
	
	/*
	 *  Mapper class to read the AVRO container file with your sessions
	 *  and then doing the session category counting.
	 */
	public static class MapClass 
		extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {
	
		// Text word object which is re-used.
		private Text word = new Text();
		
		// Mapper to multiple output.
		private AvroMultipleOutputs multipleOutputs;
		
		// Mapper session counter.
		private static final String SESSION_CATEGORY_GROUP = "Session Category Counts";
		
		public void setup(Context context) {
			multipleOutputs = new AvroMultipleOutputs(context); 
		}
		
		public void cleanup(Context context)
				 throws InterruptedException, IOException{
				 multipleOutputs.close();
		} 
		
		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
			String userId = key.datum().toString();
			
			Session session = value.datum();
			
			// Boolean variable to track different event types
			// which are then used for classification.
			boolean is_submitter = false;
			boolean is_sharer = false;
			boolean is_clicker = false;
			boolean is_shower = false;
			boolean is_only_visitor = true;
			
			// Filter out sessions with size greater than 1000.
			if (session.getEvents().size() > 1000) {
				return;
			}
			
			// Iterate over all the events and try finding the session category.
			for (Event event : session.getEvents()) {
				switch(event.getEventType()) {
					case CHANGE:
					case CONTACT_FORM_STATUS:
					case EDIT:
					case SUBMIT:
						is_submitter = true;
						break;
					case SHARE:
						is_sharer = true;
						break;
					case CLICK:
						is_clicker = true;
						break;
					case SHOW:
						is_shower = true;
						break;
					case VISIT:
						is_only_visitor = true;
						break;
					default:
						is_only_visitor = false;
						break;
				}
			}
			
			if (is_submitter) {
				multipleOutputs.write("Submitter", key,	value, "Submitter"); 
				context.getCounter(SESSION_CATEGORY_GROUP, "Submitter").increment(1L);
			} else if (is_sharer) {
				multipleOutputs.write("Sharer", key, value, "Sharer"); 
				context.getCounter(SESSION_CATEGORY_GROUP, "Sharer").increment(1L);
			} else if (is_clicker) {
				multipleOutputs.write("Clicker", key,	value, "Clicker"); 
				context.getCounter(SESSION_CATEGORY_GROUP, "Clicker").increment(1L);
			} else if (is_shower) {
				multipleOutputs.write("Shower", key,	value, "Shower"); 
				context.getCounter(SESSION_CATEGORY_GROUP, "Shower").increment(1L);
			} else if (is_only_visitor) {
				multipleOutputs.write("Visitor", key,	value, "Visitor"); 
				context.getCounter(SESSION_CATEGORY_GROUP, "Visitor").increment(1L);
			} else {
				multipleOutputs.write("Other", key,	value, "Other"); 
				context.getCounter(SESSION_CATEGORY_GROUP, "Other").increment(1L);
			}
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MapOnlyFilterSessions <input path1> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = new Job(conf, "MapOnlyFilterSession");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(MapOnlyFilterSessions.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		// Mapper.
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		job.setMapperClass(MapClass.class);
		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());
		
		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());
		
		// Specify output key schema for avro output type.
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		// Set avro multiple output related setting.
		AvroMultipleOutputs.addNamedOutput(job, "Submitter", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Sharer", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Clicker", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Shower", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Visitor", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "Other", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
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
		int res = ToolRunner.run(new Configuration(), new MapOnlyFilterSessions(), args);
		System.exit(res);
	}
}