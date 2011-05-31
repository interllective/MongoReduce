package org.apache.hadoop.contrib.mongoreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;


public class MongoRecordReader extends RecordReader<Text, DBObject> {

	
	private DBCursor cursor;
	private float totalResults;
	private float resultsRead;
	private String key;
	private DBObject value;
	
	@Override
	public void close() throws IOException {
		this.cursor.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(key);
	}

	@Override
	public DBObject getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return resultsRead / totalResults;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		// read from one of the locations for this shard
		for(String loc : split.getLocations()) {
			try {
				String[] parts = loc.split(":");
				
				// default port for sharded server
				int port = 27018;
				if(parts.length > 1) 
					port = Integer.parseInt(parts[1]);
				
				Mongo mongo = new Mongo(parts[0], port);
				
				Configuration conf = context.getConfiguration();
				String database = conf.get("database");
				String collection = conf.get("collection");
				String query = conf.get("query", "");
				String select = conf.get("select","");
				
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
					
				totalResults = cursor.count();
				resultsRead = 0.0f;
				
				break;
			}
			catch (IOException e) {
				
			}
		}
		
		// TODO: do something to acknowledge that we couldn't read this shard ...
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean hadMore = cursor.hasNext();
		if(hadMore) {
			value = cursor.next();
			key = ((ObjectId)value.get("_id")).toString();
			resultsRead++;
		}
		
		return hadMore;
	}
}
