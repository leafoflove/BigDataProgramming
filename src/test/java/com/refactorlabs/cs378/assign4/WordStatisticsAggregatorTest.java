package com.refactorlabs.cs378.assign4;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for the multiple statistics writing program.
 * 
 * @author gnanda
 */

public class WordStatisticsAggregatorTest {
	MapDriver<Text, Text, Text, WordStatisticsWritable> mapDriver;
	ReduceDriver<Text, WordStatisticsWritable, Text, WordStatisticsWritable> reduceDriver;
	
	@Before
	public void setup() {
		WordStatisticsAggregator.MapClass mapper = new WordStatisticsAggregator.MapClass();
		WordStatistics.ReduceClass reducer = new WordStatistics.ReduceClass();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
		
	private static final String TEST_WORD = "Gaurav Gaurav";
	
	//@Test
	public void testMapClass() {
		WordStatisticsWritable longArrayWritable =
				new WordStatisticsWritable(1, 2, 4);
		mapDriver.withInput(new Text("Gaurav"), new Text(longArrayWritable.toString()));
		mapDriver.withOutput(new Text("Gaurav"), longArrayWritable);
		
		try {
			mapDriver.runTest();
		} catch(IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
	
	//@Test
	public void testReduceClass() {
		WordStatisticsWritable longArrayWritable1 = new WordStatisticsWritable(1, 3, 9);
		WordStatisticsWritable longArrayWritable2 = new WordStatisticsWritable(1, 4, 16);
		WordStatisticsWritable longArrayWritable3 = new WordStatisticsWritable(1, 2, 4);
		
		List<WordStatisticsWritable> valueList = Lists.newArrayList(longArrayWritable1, 
				longArrayWritable2, longArrayWritable3);
		reduceDriver.withInput(new Text(TEST_WORD), valueList);
		
		WordStatisticsWritable doubleArrayWritable1 = new WordStatisticsWritable(3, 9, 29);
		reduceDriver.withOutput(new Text(TEST_WORD), doubleArrayWritable1);
	
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
}