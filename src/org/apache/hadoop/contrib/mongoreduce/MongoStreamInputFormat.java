/**
 * Copyright 2011 Interllective Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.apache.hadoop.contrib.mongoreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


/** 
 * Note - this uses the old mapred classes, not mapreduce ...
 * 
 * @author aaron
 *
 */
@SuppressWarnings("deprecation")
public class MongoStreamInputFormat implements InputFormat<Text, Text>, JobConfigurable {

	
	
	public static class MongoStreamInputSplit implements InputSplit {

		String[] locations = new String[0];

		// need this
		public MongoStreamInputSplit() {
			locations = new String[0];
		}
		
		public MongoStreamInputSplit(String[] locations) {
			this.locations = locations;
		}
		
		//@Override
		public long getLength() throws IOException {
			// treat all shards as equal?
			// mongo already aggressively distributes shards uniformly
			return 1024;  // arbitrary non zero number ...
		}

		//@Override
		public String[] getLocations() throws IOException {
			return locations;
		}

		//@Override
		public void readFields(DataInput in) throws IOException {
			int len = in.readInt();
			locations = new String[len];
			for(int i=0; i < len; i++)
				locations[i] = in.readUTF();
		}

		//@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(locations.length);
			for(String loc : locations) 
				out.writeUTF(loc);
		}
		
	}
	
	//@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {

		return new MongoStreamRecordReader(split, conf);
	}

	
	/**
	 * almost identical to MongoInputFormat
	 * 
	 * just uses old API and returns Streaming Splits instead
	 */
	//@Override
	public InputSplit[] getSplits(JobConf conf, int numsplits) throws IOException {

		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		InputSplit[] ret;
		
		// in single testing mode we just hit the local db
		boolean singleTestingMode = conf.getBoolean("mongo.single.testing", false);
		if(singleTestingMode) {
			String[] hosts = {"localhost:27017"};
			splits.add(new MongoStreamInputSplit(hosts));
			
			ret = new InputSplit[1];
			return splits.toArray(ret);
		}
		
		boolean primaryOk = conf.getBoolean("mongo.input.primary_ok", false);
		
		
		// connect to global mongo through a mongos process
		Mongo m = new Mongo("localhost", 27017);
		
		// get a list of all shards and their hosts
		DB configdb = m.getDB("config");
		DBCollection shards = configdb.getCollection("shards");
		
		
		// we need to query/read/process each shard once
		for(DBObject shard : shards.find()) {
			
			System.out.println("adding shard" + shard.toString());
						
			String[] hosts = MongoInputFormat.hostsForShard((String) shard.get("host"), primaryOk);
			
			for(String h : hosts)
				System.out.println("host:" + h);
					
			InputSplit split = new MongoStreamInputSplit(hosts);
			splits.add(split);
		}
		
		ret = new InputSplit[splits.size()];
		return splits.toArray(ret);
	}

	//@Override
	public void configure(JobConf job) {
		
	}
	
	public static void main(String[] args) {
		String s = "RepShard1/10.78.82.253:27018";
		String[] parts = s.split("/");
		String[] hosts = parts[1].split(",");
		
		ArrayList<String> secs = new ArrayList<String>();
		secs.add("sdf");
		hosts = secs.toArray(new String[0]);
		
		for(String h : hosts)
			System.out.println(h);
	}
}
