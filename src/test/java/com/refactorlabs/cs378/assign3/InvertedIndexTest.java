package com.refactorlabs.cs378.assign3;

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
import com.refactorlabs.cs378.assign2.LongArrayWritable;

/**
 * Unit tests for the multiple statistics writing program.
 * 
 * @author gnanda
 */

public class InvertedIndexTest {
	MapDriver<LongWritable, Text, Text, VerseArrayWritable> mapDriver;
	ReduceDriver<Text, VerseArrayWritable, Text, VerseArrayWritable> reduceDriver;
	
	@Before
	public void setup() {
		InvertedIndex.MapClass mapper = new InvertedIndex.MapClass();
		InvertedIndex.ReduceClass reducer = new InvertedIndex.ReduceClass();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
		
	private static final String TEST_STRING1 = "Ezekiel:3:23 Then";
	private static final String TEST_STRING2 = "Ezekiel:3:13 Then";
	
	//@Test
	public void testMapClass() {
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_STRING1));
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_STRING2));
		
		Verse v1 = new Verse("Ezekiel:3:23");
		Verse v2 = new Verse("Ezekiel:3:13");
		VerseArrayWritable writable = new VerseArrayWritable(new Verse[] {v1, v2});
		mapDriver.withOutput(new Text("then"), writable);
		
		try {
			mapDriver.runTest();
		} catch(IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
	
	//@Test
	public void testReduceClass() {
		Verse ve1 = new Verse("Ezekiel:3:23");
		Verse ve2 = new Verse("Ezekiel:3:13");
		
		VerseArrayWritable v1 = new VerseArrayWritable(new Verse[] {ve1});
		VerseArrayWritable v2 = new VerseArrayWritable(new Verse[] {ve2});
	
		List<VerseArrayWritable> valueList = Lists.newArrayList(v1, v2);
		reduceDriver.withInput(new Text("then"), valueList);
		
		VerseArrayWritable v3 = new VerseArrayWritable(new Verse[] {ve1, ve2});
		reduceDriver.withOutput(new Text("then"), v3);
		
		try {
			reduceDriver.runTest();
		} catch(IOException ioe) {
			Assert.fail("IOException from reducer: " + ioe.getMessage());
		}
	}
	
}