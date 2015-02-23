package com.refactorlabs.cs378.assign4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WordStatisticsWritable implements Writable {
	// List down the count fields.
	private long documentCount;
	private long totalCount;
	private long sumOfSquares;
	
	// List down the statistics fields.
	private double mean;
	private double variance;
	
	private static final String COMMA = ",";
	
	WordStatisticsWritable() { }
	
	WordStatisticsWritable(long documentCount, long totalCount, long sumOfSquares) {
		this.documentCount = documentCount;
		this.totalCount = totalCount;
		this.sumOfSquares = sumOfSquares;
		
		this.updateStatistics();
	}
	
	public static WordStatisticsWritable fromString(String str) {
		WordStatisticsWritable writable = new WordStatisticsWritable();
		
		String[] values = str.split(",");
		writable.documentCount = Long.valueOf(values[0]);
		writable.totalCount = Long.valueOf(values[1]);
		writable.sumOfSquares = Long.valueOf(values[2]);
		writable.mean = Double.valueOf(values[3]);
		writable.variance = Double.valueOf(values[4]);
		
		return writable;
	}
	
	// Private function to re-compute mean and variance.
	private void updateStatistics() {
		this.mean = 1.0 * this.totalCount / this.documentCount;
		this.variance = (1.0 * this.sumOfSquares / this.documentCount) - (1.0 * this.mean * this.mean);
	}
	
	// Update the counts and call the update statisitcs from within.
	void updateCounts(long documentCount, long totalCount, long sumOfSquares) {
		this.documentCount = documentCount;
		this.totalCount = totalCount;
		this.sumOfSquares = sumOfSquares;
		
		this.updateStatistics();
	}
	
	void addWordStatisticsWritable(WordStatisticsWritable other) {
		this.documentCount += other.documentCount;
		this.totalCount += other.totalCount;
		this.sumOfSquares += other.sumOfSquares;
		
		this.updateStatistics();
	}
	
	void reset() {
		this.documentCount = 0;
		this.totalCount = 0;
		this.sumOfSquares = 0;
		this.mean = 0;
		this.variance = 0;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.documentCount = in.readLong();
		this.totalCount = in.readLong();
		this.sumOfSquares = in.readLong();
		
		// Discard the doubles and re-computer the mean and variance, just to be sure.
		// Ideally, this is not required though.
		in.readDouble(); in.readDouble();
		this.updateStatistics();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.documentCount);
		out.writeLong(this.totalCount);
		out.writeLong(this.sumOfSquares);
		out.writeDouble(this.mean);
		out.writeDouble(this.variance);
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append(this.documentCount);
		builder.append(COMMA);
		builder.append(this.totalCount);
		builder.append(COMMA);
		builder.append(this.sumOfSquares);
		builder.append(COMMA);
		builder.append(this.mean);
		builder.append(COMMA);
		builder.append(this.variance);
		
		return builder.toString();
	}
}
