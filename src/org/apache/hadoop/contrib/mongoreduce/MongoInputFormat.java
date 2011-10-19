/**
 * Copyright 2011 Interllective Inc.
 * 
 */
package org.apache.hadoop.contrib.mongoreduce;

import java.io.IOException;
import java.net.UnknownHostException;
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
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;


public class MongoInputFormat extends InputFormat<Text, DBObject> {

	static DBObject cmd;
	
	{
		BasicDBObjectBuilder cmdBuilder = new BasicDBObjectBuilder();
		cmdBuilder.add("isMaster", 1);
		cmd = cmdBuilder.get();
	}
	
	
	public static String[] hostsForShard(String shardName, boolean primaryOk) throws UnknownHostException, MongoException {
		
		ArrayList<String> hosts = new ArrayList<String>();
		
		String[] parts = shardName.split("/");
		if(parts.length == 1) { // no replicas
			hosts.add(shardName);
		}
		else { // replicas
			
			// get first or only host listed
			String host = parts[1].split(",")[0];
			Mongo h = new Mongo(host);
			List<ServerAddress> addresses = h.getServerAddressList();
			h.close();
			h = null;
			
			// only one node in replica set ... - use it
			if(addresses.size() == 1) {
				ServerAddress addr = addresses.get(0);
				hosts.add(addr.getHost() + ":" + Integer.toString(addr.getPort()));
			}
			
			else {
				for(ServerAddress addr : addresses) {
					
					// use secondaries and primaries
					if(primaryOk) {
						hosts.add(addr.getHost() + ":" + Integer.toString(addr.getPort()));
					}
					
					// only use secondaries
					else {
						String haddr = addr.getHost() + ":" + Integer.toString(addr.getPort());
						h = new Mongo(haddr);
						if(!(Boolean)h.getDB(h.getDatabaseNames().get(0)).command(cmd).get("ismaster")) {
							hosts.add(haddr);
						}
					}
				}
			}
		}
		
		return hosts.toArray(new String[0]);
	}


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
		
		
		// connect to global mongo through a mongos process
		Mongo m = new Mongo("localhost", 27017);
		
		// get a list of all shards and their hosts
		// TODO: add notification if config db not found / db is not sharded
		DB configdb = m.getDB("config");
		DBCollection shards = configdb.getCollection("shards");
		
		
		// we need to query/read/process each shard once
		for(DBObject shard : shards.find()) {
			
			System.out.println("adding shard" + shard.toString());
			
			String[] hosts = hostsForShard((String) shard.get("host"), primaryOk);
			for(String host : hosts) 
				System.out.print(host + " ");
			System.out.println();
					
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
