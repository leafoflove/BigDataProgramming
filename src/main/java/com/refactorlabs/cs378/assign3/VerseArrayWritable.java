package com.refactorlabs.cs378.assign3;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is just a collection of the Writable Verses.
 * 
 * @author gnanda
 *
 */
public class VerseArrayWritable extends ArrayWritable {

	public VerseArrayWritable() {
		super(Verse.class);
	}
	
	public VerseArrayWritable(Object[] objArr) {
		super(Verse.class);
		Verse[] arr = new Verse[objArr.length];
		for (int i=0; i < objArr.length; ++i) {
			arr[i] = (Verse)objArr[i];
		}
		set(arr);
	}
	
	public VerseArrayWritable(Verse[] arr) {
		super(Verse.class);
		set(arr);
	}
	
	public String toString() {
		Writable[] wValues = get();
		Arrays.sort(wValues);
		return StringUtils.join(wValues, ",");
	}
}