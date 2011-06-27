/**
 * Copyright 2011 Interllective Inc.
 * 
 */
package org.apache.hadoop.contrib.mongoreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;


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
		
		Configuration conf = context.getConfiguration();
		
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		
		// in single testing mode we just hit the local db
		boolean singleTestingMode = conf.getBoolean("mongo.single.testing", false);
		if(singleTestingMode) {
			String[] hosts = {"localhost:27017"};
			splits.add(new MongoInputSplit(hosts));
			
			return splits;
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
			
			InputSplit split = new MongoInputSplit(hosts);
			splits.add(split);
		}
		
		return splits;
	}
	
	// TODO: add verification to these calls and fail early if a problem is found
	
	/**
	 * Specifies which MongoDB database the mapper will process
	 *  
	 * @param job
	 * @param db
	 */
	public static void setDatabase(Job job, String db) {
		job.getConfiguration().set("mongo.input.database", db);
	}

	/**
	 * Specifies which MongoDB collection the mapper will process
	 * 
	 * @param job
	 * @param cl
	 */
	public static void setCollection(Job job, String cl) {
		job.getConfiguration().set("mongo.input.collection", cl);
	}

	/**
	 * A JSON string representing a MongoDB query to be performed by each 
	 * MongoDB server. The matching documents are passed to Map()
	 * 
	 * @param job
	 * @param query
	 */
	public static void setQuery(Job job, String query) {
		// quickly validate query
		JSON.parse(query);
		job.getConfiguration().set("mongo.input.query", query);
	}

	/**
	 * A JSON string representing the fields selected from documents
	 * returned from MongoDB
	 * 
	 * MongoDB performs this selection before they are passed to Map()
	 * 
	 * @param job
	 * @param select
	 */
	public static void setSelect(Job job, String select) {
		JSON.parse(select);
		job.getConfiguration().set("mongo.input.select", select);
	}

	/**
	 * Allows MapReduce to read from MongoDB Primary servers
	 * The default is false, making MapReduce read from secondaries, reducing
	 * the load on primary servers, but potentially reading slightly stale data.
	 * 
	 * Without setting Primary OK to true, failovers can only occur among secondaries. 
	 * If there is only one secondary for a given shard, and reading from that shard
	 * fails, and primaryOK is not set, that input split will never be read.
	 * 
	 * PrimaryOK gives MapReduce more servers to fall back on in case of failure.
	 * 
	 * @param job
	 * @param b
	 */
	public static void setPrimaryOk(Job job, boolean b) {
		job.getConfiguration().setBoolean("mongo.input.primary_ok", b);
	}
	
	/**
	 * Used to tell MongoInputFormat to read from a single, local database
	 * only for testing
	 * 
	 * @param job
	 * @param s
	 */
	public static void setSingleTestingMode(Job job, boolean s) {
		job.getConfiguration().setBoolean("mongo.single.testing", s);
	}
	
}
