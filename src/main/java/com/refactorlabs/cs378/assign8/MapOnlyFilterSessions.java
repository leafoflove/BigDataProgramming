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

import com.refactorlabs.cs378.assign7.VinImpressionCounts;
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
		extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {
	
		// Text word object which is re-used.
		private Text word = new Text();
		
		// Mapper session counter.
		private static final String SESSION_CATEGORY_GROUP = "Session Category Counts";
		
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
				context.getCounter(SESSION_CATEGORY_GROUP, "Submitter").increment(1L);
			} else if (is_sharer) {
				context.getCounter(SESSION_CATEGORY_GROUP, "Sharer").increment(1L);
			} else if (is_clicker) {
				context.getCounter(SESSION_CATEGORY_GROUP, "Clicker").increment(1L);
			} else if (is_shower) {
				context.getCounter(SESSION_CATEGORY_GROUP, "Shower").increment(1L);
			} else if (is_only_visitor) {
				context.getCounter(SESSION_CATEGORY_GROUP, "OnlyVisitor").increment(1L);
			} else {
				context.getCounter(SESSION_CATEGORY_GROUP, "Other").increment(1L);
			}
		}
	}
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}