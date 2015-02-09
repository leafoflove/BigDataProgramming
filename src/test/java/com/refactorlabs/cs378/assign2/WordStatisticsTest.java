package com.refactorlabs.cs378.assign2;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.assign1.WordCount;

/**
 * Unit tests for the multiple statistics writing program.
 * 
 * @author gnanda
 */

public class WordStatisticsTest {
	MapDriver<LongWritable, Text, Text, LongArrayWritable> mapDriver;
	ReduceDriver<Text, LongArrayWritable, Text, DoubleArrayWritable> reduceDriver;
	
	@Before
	public void setup() {
		WordStatistics.MapClass mapper = new WordStatistics.MapClass();
		WordStatistics.ReduceClass reducer = new WordStatistics.ReduceClass();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
		
	private static final String TEST_WORD = "Gaurav --gAurav_";
	
	@Test
	public void testMapClass() {
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		
		LongArrayWritable longArrayWritable =
				new LongArrayWritable();
		longArrayWritable.setValueArray(new long[] {1, 2, 4});
		mapDriver.withOutput(new Text("gaurav"), longArrayWritable);
		
		try {
			mapDriver.runTest();
		} catch(IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
	
	@Test
	public void testReduceClass() {
		LongArrayWritable longArrayWritable1 = new LongArrayWritable();
		LongArrayWritable longArrayWritable2 = new LongArrayWritable();
		LongArrayWritable longArrayWritable3 = new LongArrayWritable();
		
		longArrayWritable1.setValueArray(new long[] {1, 3, 9});
		longArrayWritable2.setValueArray(new long[] {1, 4, 16});
		longArrayWritable3.setValueArray(new long[] {1, 2, 4});
		
		List<LongArrayWritable> valueList = Lists.newArrayList(longArrayWritable1, 
				longArrayWritable2, longArrayWritable3);
		reduceDriver.withInput(new Text(TEST_WORD), valueList);
		
		DoubleArrayWritable doubleArrayWritable1 = new DoubleArrayWritable();
		doubleArrayWritable1.setValueArray(new double[] {3, 3, 0.6667});
		reduceDriver.withOutput(new Text(TEST_WORD), doubleArrayWritable1);
	
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
}