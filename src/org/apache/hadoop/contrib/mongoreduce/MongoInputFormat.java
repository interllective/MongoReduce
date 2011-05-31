package org.apache.hadoop.contrib.mongoreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


public class MongoInputFormat extends InputFormat<Text, DBObject> {

	@Override
	public RecordReader<Text, DBObject> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new MongoRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
				
		// connect to global mongo through a mongos process
		Mongo m = new Mongo("localhost", 27017);
		
		// get a list of all shards and their hosts
		DB configdb = m.getDB("config");
		DBCollection shards = configdb.getCollection("shards");
		
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		
		// we need to query/read/process each shard once
		for(DBObject shard : shards.find()) {
			
			System.out.println("adding shard" + shard.toString());
			
			// parse addresses out
			String hostString = (String) shard.get("host");
			String[] parts = hostString.split("/");
			String[] hosts;
			if(parts.length > 1) { // we have replica sets
				// optionally find out which is the primary and leave it out
				// to avoid reading from primary
				hosts = parts[1].split(",");
			}
			else {
				hosts = parts[0].split(",");
			}
			
			InputSplit split = new MongoInputSplit(hosts);
			splits.add(split);
		}
		
		return splits;
	}
	
	public static void setDatabase(Job job, String db) {
		job.getConfiguration().set("mongo.input.database", db);
	}

	public static void setCollection(Job job, String cl) {
		job.getConfiguration().set("mongo.input.database", cl);
	}
	
}
