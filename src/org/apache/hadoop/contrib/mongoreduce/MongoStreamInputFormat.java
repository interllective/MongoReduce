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

import com.mongodb.BasicDBObjectBuilder;
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
public class MongoStreamInputFormat implements InputFormat<Text, Text>, JobConfigurable {

	
	public static class MongoStreamInputSplit implements InputSplit {

		String[] locations;

		// need this
		public MongoStreamInputSplit() {
			
		}
		
		public MongoStreamInputSplit(String[] locations) {
			this.locations = locations;
		}
		
		@Override
		public long getLength() throws IOException {
			// treat all shards as equal?
			// mongo already aggressively distributes shards uniformly
			return 1024;  // arbitrary non zero number ...
		}

		@Override
		public String[] getLocations() throws IOException {
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
	
	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {

		return new MongoStreamRecordReader(split, conf);
	}

	@Override
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
		BasicDBObjectBuilder cmdBuilder = new BasicDBObjectBuilder();
		cmdBuilder.add("isMaster", 1);
		DBObject cmd = cmdBuilder.get();
		
		// connect to global mongo through a mongos process
		Mongo m = new Mongo("localhost", 27017);
		
		// get a list of all shards and their hosts
		DB configdb = m.getDB("config");
		DBCollection shards = configdb.getCollection("shards");
		
		
		// we need to query/read/process each shard once
		for(DBObject shard : shards.find()) {
			
			System.out.println("adding shard" + shard.toString());
			
			// parse addresses out
			String hostString = (String) shard.get("host");
			String[] parts = hostString.split("/");
			String[] hosts;
			if(parts.length > 1) { // we have replica sets
				hosts = parts[1].split(",");
				
				if(!primaryOk) {
					ArrayList<String> secondaries = new ArrayList<String>();
					
					// determine secondaries
					for(String host : hosts) {
						Mongo h = new Mongo(host);
						boolean ismaster = (Boolean)h.getDB(h.getDatabaseNames().get(0)).command(cmd).get("ismaster");
						if(!ismaster)
							secondaries.add(host);
					}
					
					hosts = secondaries.toArray(hosts);
				}
			}
			else {
				hosts = parts[0].split(",");
			}
			
			InputSplit split = new MongoStreamInputSplit(hosts);
			splits.add(split);
		}
		
		ret = new InputSplit[splits.size()];
		return splits.toArray(ret);
	}

	@Override
	public void configure(JobConf job) {
		
	}
}
