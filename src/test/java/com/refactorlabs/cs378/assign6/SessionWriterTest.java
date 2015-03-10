package com.refactorlabs.cs378.assign6;

import com.google.common.collect.Lists;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.refactorlabs.cs378.assign5.WordStatisticsData;
import com.refactorlabs.cs378.sessions.*;
/**
 * Unit test for the SessionWriter map-reduce program.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class SessionWriterTest {

	MapDriver<LongWritable, Text, Text, AvroValue<Session>> mapDriver;
	//ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setup() {
		// We did not need to import SessionWriter and all, because they are in
		// the same package (not the same folder).
		SessionWriter.MapClass mapper = new SessionWriter.MapClass();
		SessionWriter.ReduceClass reducer = new SessionWriter.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		Configuration conf = mapDriver.getConfiguration();
		
		//Copy over the default io.serializations. If you don't do this then you will 
	    //not be able to deserialize the inputs to the mapper
	    String[] strings = mapDriver.getConfiguration().getStrings("io.serializations");
	    String[] newStrings = new String[strings.length +1];
	    System.arraycopy( strings, 0, newStrings, 0, strings.length );
	    newStrings[newStrings.length-1] = AvroSerialization.class.getName();

	    //Now you have to configure AvroSerialization by sepecifying the key
	    //writer Schema and the value writer schema.
	    //Schema.create(Schema.Type.STRING), WordStatisticsData.getClassSchema()
	    mapDriver.getConfiguration().setStrings("io.serializations", newStrings);
	    mapDriver.getConfiguration().setStrings("avro.serialization.key.writer.schema", Schema.create(Schema.Type.LONG).toString(true));
	    mapDriver.getConfiguration().setStrings("avro.serialization.value.writer.schema", Session.getClassSchema().toString(true));
		//reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static final String TEST_WORD = "268483462	contact form error	market report view	http://www.emichvw.com/searchused.aspx?make=Volkswagen	www.emichvw.com	2014-12-05 15:45:48.000000	Denver	Colorado	WVWGK93C76P204894	Used	2006	Volkswagen	Passat	2.0T	Sedan	null	null	7995.0000	111336	24	Blue	Black	2.0 L	4 Cyl	Manual	FWD	Gasoline	1	f	f	f	4-Wheel Disc Brakes:4-Wheel Independent Suspension:AM/FM:Adjustable Steering Wheel:Air Conditioning:Anti-lock Brakes:Anti-theft System:CD (Single Disc):Compass:Cruise Control:Front Airbags (Driver):Front Side Airbags (Driver):Heated Mirrors:Intermittent Wipers:Keyless Entry:Leatherette Seats:MP3:Power Mirrors:Power Steering:Power Windows:Reading Lights:Rear Window Defroster:Split/Folding Seats:Tachometer:Thermometer:Tire Pressure Monitoring System:Trip Computer:Vanity Mirror/Light:8 Speakers:Body Colored Bumpers:Body Side Moldings:Braking Assist:Door Bin:Front Airbags (Passenger):Front Anti-Roll Bar:Front Bucket Seats:Front Center Armrest:Front Side Airbags (Passenger):Head Restraint Whiplash Protection:Illuminated Entry:Overhead Airbag:Overhead Console:Passenger Sensing Airbag:Rear Anti-Roll Bar:Rear Center Armrest:Speed-Sensing Steering:Stability Control:Traction Control:Turn Signal Mirrors";

	//@Test
	public void testMapClass() {
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		Session.Builder sessionBuilder = Session.newBuilder();
		ArrayList<Event> events = new ArrayList<Event>();
		Event ev = new Event();
		ev.event_type = EventType.valueOf("VISIT");
		ev.event_subtype = EventSubtype.valueOf("BADGES");
		
		List<java.lang.CharSequence> features = new ArrayList<java.lang.CharSequence>();
		ev.features =  features;
		events.add(ev);
		sessionBuilder.setEvents(events);
		sessionBuilder.setUserId("268483462");
		//mapDriver.withOutput(new Text("268483462"), new AvroValue(sessionBuilder.build()));
		try {
			mapDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

	//@Test
	public void testReduceClass() {
//		Session.Builder sessionBuilder = Session.newBuilder();
//		ArrayList<Event> events = new ArrayList<Event>();
//		Event ev = new Event();
//		ev.event_type = EventType.valueOf("VISIT");
//		ev.event_subtype = EventSubtype.valueOf("BADGES");
//		
//		List<java.lang.CharSequence> features = new ArrayList<java.lang.CharSequence>();
//		ev.features =  features;
//		events.add(ev);
//		sessionBuilder.setEvents(events);
//		sessionBuilder.setUserId("268483462");
//		
//		List<AvroValue<Session>> valueList = Lists.newArrayList(new AvroValue(sessionBuilder.build(), new AvroValue(sessionBuilder.build()));
//		reduceDriver.withInput(new Text(TEST_WORD), valueList);
//		reduceDriver.withOutput(new Text(TEST_WORD), new LongWritable(3L));
//		try {
//			reduceDriver.runTest();
//		} catch (IOException ioe) {
//			Assert.fail("IOException from mapper: " + ioe.getMessage());
//		}
	}
}
