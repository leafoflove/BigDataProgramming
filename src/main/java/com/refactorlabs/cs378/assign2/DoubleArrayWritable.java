package com.refactorlabs.cs378.assign2;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleArrayWritable extends ArrayWritable implements WritableComparable {
	
	// This is to set proper type of the ArraayWritable class.
	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}
	
	// Read in all the writables and convert them into
	// a Double array.
	public double[] getValueArray() {
		Writable[] wValues = get();
		double[] values = new double[wValues.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = ((DoubleWritable)wValues[i]).get();
		}
		return values;
	}
	
	// Send in a array of double values, convert them
	// into a Writable[] array and update the DoubleArrayWritable.
	public void setValueArray(double[] values) {
		Writable[] wValues = new DoubleWritable[values.length];
		for (int i = 0; i < values.length; i++) {
			wValues[i] = new DoubleWritable(values[i]);
		}
		set(wValues);
	}
	
	// Override toString () and equals() method.
	public String toString() {
		double[] values = getValueArray();
		
		StringBuilder strBuilder = new StringBuilder();
		
		StringBuilder breaker = new StringBuilder();
		for (int i=0; i < values.length; ++i) {
			strBuilder.append(breaker);
			strBuilder.append(String.format("%.4f", values[i]));
			breaker.setLength(1);
			breaker.setCharAt(0, ',');
		}
		
		return strBuilder.toString();
	}
	
	/**
	 * If number of values in the long array are equal and
	 * difference in each value is less than .0001 then return true, else false.
	 */
	public boolean equals(Object obj) {
		DoubleArrayWritable other = (DoubleArrayWritable) obj;
		Writable[] otherWValues = other.get();
		Writable[] thisWValues = get();
		
		if (otherWValues.length != thisWValues.length) {
			return false;
		}
		
		for (int i=0; i < thisWValues.length; ++i) {
			if ( ((DoubleWritable)otherWValues[i]).get() - 
					((DoubleWritable)thisWValues[i]).get() > .0001) {
				return false;
			}
		}	
		return true;
	}

	@Override
	/*
	 * Coming from WritableComparable interface. Returning 0
	 * if the objects are same, otherwise returning some random
	 * value, i.e 1 here.(non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		if (this.equals(arg0)) {
			return 0;
		} else {
			return 1;
		}
	}
}
