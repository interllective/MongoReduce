/**
 * Copyright 2011 Interllective Inc.
 * 
 */
package org.apache.hadoop.contrib.mongoreduce;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * writes all go through the global database
 * @author aaron
 *
 */

public class MongoRecordWriter extends RecordWriter<Text, DBObject> {

	
	private DBCollection coll;
	private Mongo mongo;

	
	public MongoRecordWriter(String database, String collection) throws IOException {
	
		
		// connect to local mongos process
		try {
			mongo = new Mongo("localhost", 27017);
			coll = mongo.getDB(database).getCollection(collection);
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
			throw new IOException(e.getMessage());
		} catch (MongoException e) {
			
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
		 
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {

		mongo.close();

	}

	@Override
	public void write(Text key, DBObject value) throws IOException,
			InterruptedException {
		
		value.put("_id", key.toString()); // new ObjectId(getValidKey(key.toString())));
		
		coll.save(value);
	}

}
