/**
 * Copyright 2011 Interllective Inc.
 * 
 * This class exists to produce JSON strings from mongo to be consumed by streaming scripts
 * 
 */
package org.apache.hadoop.contrib.mongoreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

@SuppressWarnings("deprecation")
public class MongoStreamRecordReader implements RecordReader<Text, Text> {

	private DBCursor cursor;
	private float totalResults;
	private float resultsRead;
	
	
	public MongoStreamRecordReader(InputSplit split, JobConf conf) throws IOException {
		// read from one of the locations for this shard
		
		for(String loc : split.getLocations()) {
			try {
				String[] parts = loc.split(":");
				
				// default port for sharded server
				int port = 27018;
				if(parts.length > 1) 
					port = Integer.parseInt(parts[1]);
				
				Mongo mongo = new Mongo(parts[0], port);
				// figure out if we can read from this server
						
				// allow reading from secondaries
				mongo.slaveOk();
				
				String database = conf.get("mongo.input.database");
				if(database == null)
					throw new IOException("database is null");
				
				String collection = conf.get("mongo.input.collection");
				if(collection == null)
					throw new IOException("collection is null");
				
				String query = conf.get("mongo.input.query", "");
				String select = conf.get("mongo.input.select","");
				
				if(!query.equals("")) {
					DBObject q = (DBObject) JSON.parse(query);
					
					if(!select.equals("")) {
						DBObject s = (DBObject) JSON.parse(select);
						cursor = mongo.getDB(database).getCollection(collection).find(q, s);
					}
					else {
						cursor = mongo.getDB(database).getCollection(collection).find(q);
					}
				}
				else {
					if(!select.equals("")) {
						DBObject s = (DBObject) JSON.parse(select);
						cursor = mongo.getDB(database).getCollection(collection).find(new BasicDBObject(), s);
					}
					else {
						cursor = mongo.getDB(database).getCollection(collection).find();
					}
				}
				
				cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
				
				// thanks mongo, for this handy method
				totalResults = cursor.count();
				resultsRead = 0;
				
				return;
			}
			catch (IOException e) {
				System.out.println("unable to connect to location " + loc + ", trying others ...");
				System.out.println(e.getMessage());
			}
		}
		
		throw new IOException("unable to connect to any locations for shard");
	}
	
	@Override
	public void close() throws IOException {
		cursor.close();
	}


	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return (long)resultsRead;
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		boolean hadMore = cursor.hasNext();
		if(hadMore) {
			DBObject v = cursor.next();
			key.set(((ObjectId)v.get("_id")).toString().getBytes("UTF-8"));
			
			// remove object ID from DBObject
			v.removeField("_id");
			value.set(v.toString().getBytes("UTF-8"));
			resultsRead++;
		}
		
		return hadMore;
	}


	@Override
	public float getProgress() throws IOException {
		return resultsRead / totalResults;
	}
}

