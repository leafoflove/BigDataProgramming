package com.refactorlabs.cs378.assign3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

/**
 * A verse consists of book:chapter:verse. We would break these three and
 * persist them in the verse class. This would be helpful while 
 * we try to operations like sort and all. This class is doing dual function
 * of being a primary class and a writable class.
 * 
 * There is no separate VerseWritable class, this class does both things.
 * 
 * @author gnanda
 *
 */
public class Verse implements Comparable<Verse>, Writable {
	private String book;
	private int chapter;
	private int verse;
	
	// Empty Constructor.
	public Verse() {	
	}
	
	public Verse(String book, int chapter, int verse) {
		this.book = book;
		this.chapter = chapter;
		this.verse = verse;
	}
	
	// Create a verse object from the string.	
	public Verse(String id) {
		String[] strings = StringUtils.split(id, ":");
		book = strings[0];
		chapter = Integer.parseInt(strings[1]);
		verse = Integer.parseInt(strings[2]);
	}

	// This would be useful for the extra credit.
	@Override
	public int compareTo(Verse o) {
		// There is only one single book, so we would just bypass that.
		if (this.chapter < o.chapter) {
			return -1;
		} else if (this.chapter > o.chapter) {
			return 1;
		} else {
			if (this.verse < o.verse) {
				return -1;
			} else if (this.verse > o.verse ) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		book = in.readUTF();
		chapter = in.readInt();
		verse = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(book);
		out.writeInt(chapter);
		out.writeInt(verse);
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(book);
		str.append(":");
		str.append(chapter);
		str.append(":");
		str.append(verse);
		return str.toString();
	}
	
}
