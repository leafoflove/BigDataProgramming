package com.refactorlabs.cs378.assign6;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit test for the WordCount map-reduce program.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCountTest {

	MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setup() {
		// We did not need to import WordCount and all, because they are in
		// the same package (not the same folder).
		WordCount.MapClass mapper = new WordCount.MapClass();
		WordCount.ReduceClass reducer = new WordCount.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static final String TEST_WORD = "268483462	contact form error	market report view	http://www.emichvw.com/searchused.aspx?make=Volkswagen	www.emichvw.com	2014-12-05 15:45:48.000000	Denver	Colorado	WVWGK93C76P204894	Used	2006	Volkswagen	Passat	2.0T	Sedan	null	null	7995.0000	111336	24	Blue	Black	2.0 L	4 Cyl	Manual	FWD	Gasoline	1	f	f	f	4-Wheel Disc Brakes:4-Wheel Independent Suspension:AM/FM:Adjustable Steering Wheel:Air Conditioning:Anti-lock Brakes:Anti-theft System:CD (Single Disc):Compass:Cruise Control:Front Airbags (Driver):Front Side Airbags (Driver):Heated Mirrors:Intermittent Wipers:Keyless Entry:Leatherette Seats:MP3:Power Mirrors:Power Steering:Power Windows:Reading Lights:Rear Window Defroster:Split/Folding Seats:Tachometer:Thermometer:Tire Pressure Monitoring System:Trip Computer:Vanity Mirror/Light:8 Speakers:Body Colored Bumpers:Body Side Moldings:Braking Assist:Door Bin:Front Airbags (Passenger):Front Anti-Roll Bar:Front Bucket Seats:Front Center Armrest:Front Side Airbags (Passenger):Head Restraint Whiplash Protection:Illuminated Entry:Overhead Airbag:Overhead Console:Passenger Sensing Airbag:Rear Anti-Roll Bar:Rear Center Armrest:Speed-Sensing Steering:Stability Control:Traction Control:Turn Signal Mirrors";

	//@Test
	public void testMapClass() {
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		mapDriver.withOutput(new Text(TEST_WORD), WordCount.ONE);
		try {
			mapDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

	//@Test
	public void testReduceClass() {
		List<LongWritable> valueList = Lists.newArrayList(WordCount.ONE, WordCount.ONE, WordCount.ONE);
		reduceDriver.withInput(new Text(TEST_WORD), valueList);
		reduceDriver.withOutput(new Text(TEST_WORD), new LongWritable(3L));
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
}
