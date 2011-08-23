package org.apache.hadoop.contrib.mongoreduce;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class MongoStreamRecordWriter implements RecordWriter<Text, Text> {

	private Mongo mongo;
	private DBCollection coll;

	public MongoStreamRecordWriter(String database, String collection)
			throws IOException {
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
	public void write(Text key, Text value) {
		
		DBObject objValue = (DBObject)JSON.parse(value.toString());
		
		objValue.put("_id", key.toString());
		
		coll.save(objValue);
	}


	@Override
	public void close(Reporter reporter) throws IOException {
		mongo.close();
		
	}


}
