package com.refactorlabs.cs378.assign2;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class LongArrayWritable extends ArrayWritable implements WritableComparable {
	
	// This is to set proper type of the ArraayWritable class.
	public LongArrayWritable() {
		super(LongWritable.class);
	}
	
	// Read in all the writables and convert them into
	// a long array.
	public long[] getValueArray() {
		Writable[] wValues = get();
		long[] values = new long[wValues.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = ((LongWritable)wValues[i]).get();
		}
		return values;
	}
	
	// Send in a array of long values, convert them
	// into a Writable[] array and update the LongArrayWritable.
	public void setValueArray(long[] values) {
		Writable[] wValues = new LongWritable[values.length];
		for (int i = 0; i < values.length; i++) {
			wValues[i] = new LongWritable(values[i]);
		}
		set(wValues);
	}
	
	// Override toString () and equals() method.
	public String toString() {
		long[] values = getValueArray();
		
		StringBuilder strBuilder = new StringBuilder();
		
		StringBuilder breaker = new StringBuilder();
		for (int i=0; i < values.length; ++i) {
			strBuilder.append(breaker);
			strBuilder.append(values[i]);
			breaker.setLength(1);
			breaker.setCharAt(0, ',');
		}
		
		return strBuilder.toString();
	}
	
	public boolean equals(Object obj) {
		LongArrayWritable other = (LongArrayWritable) obj;
		Writable[] otherWValues = other.get();
		Writable[] thisWValues = get();
		
		if (otherWValues.length != thisWValues.length) {
			return false;
		}
		
		for (int i=0; i < thisWValues.length; ++i) {
			if ( ((LongWritable)otherWValues[i]).get() != 
					((LongWritable)thisWValues[i]).get()) {
				return false;
			}
		}
		
		return true;
	}

	@Override
	// Coming from WritableComparable interface.
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		if (this.equals(arg0)) {
			return 0;
		} else {
			return 1;
		}
	}
}
