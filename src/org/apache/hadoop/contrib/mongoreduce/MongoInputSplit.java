/**
 * Copyright 2011 Interllective Inc.
 * 
 */
package org.apache.hadoop.contrib.mongoreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * apparently all InputSplit subclasses need to be Writable as well ..
 * 
 * @author aaron
 *
 */
public class MongoInputSplit extends InputSplit implements Writable {

	String[] locations;

	// need this
	public MongoInputSplit() {
		
	}
	
	public MongoInputSplit(String[] locations) {
		this.locations = locations;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		// treat all shards as equal?
		// mongo already aggressively distributes shards uniformly
		return 1024;  // arbitrary non zero number ...
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		
		return locations;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		locations = new String[len];
		for(int i=0; i < len; i++)
			locations[i] = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(locations.length);
		for(String loc : locations) 
			out.writeUTF(loc);
	}
	
}