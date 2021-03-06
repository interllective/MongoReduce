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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


@SuppressWarnings("deprecation")
public class MongoStreamOutputFormat implements OutputFormat<Text, Text> {

	//@Override
	public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored,
			JobConf job, String name, Progressable progress) throws IOException {
		
		String database = job.get("mongo.output.database");
		String collection = job.get("mongo.output.collection");
		
		return new MongoStreamRecordWriter(database, collection);
	}

	//@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf conf)
			throws IOException {
		
		if(conf.getBoolean("mongo.output.skip_splitting", false)) 
			return;
		
		String database = conf.get("mongo.output.database", "");
		if(database.equals("")) {
			throw new IOException("must specify a value for mongo.output.database");
		}
		
		String collection = conf.get("mongo.output.collection", "");
		if(collection.equals("")) {
			throw new IOException("must supply a value for mongo.output.collection");
		}
				
		// connect to global db
		Mongo m = new Mongo("localhost");
		DB db = m.getDB(database);
		DB admindb = m.getDB("admin");
		DB configdb = m.getDB("config");
		
		// optionally drop the existing collection
		boolean drop = conf.getBoolean("mongo.output.drop", false);
		DBCollection coll = db.getCollection(collection);
		if(drop) {
			coll.drop();
		}
		else {
			if(coll.count() > 0) {
				// don't shard an existing collection - may already be sharded ...
				return;
			}
		}
		
		// get a list of shards
		ArrayList<String> shards = new ArrayList<String>();
		for(DBObject s : configdb.getCollection("shards").find()) {
			shards.add((String)s.get("_id"));
		}
		
		if(shards.size() < 2) {
			// don't let's be silly
			return;
		}
		
		// shard the new output collection
		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
		builder.add("enableSharding", database);
		admindb.command(builder.get());
		
		builder = new BasicDBObjectBuilder();
		builder.add("shardCollection", database + "." + collection);
		
		// just shard on _id - but user gets to decide what the _id is
		builder.add("key", new BasicDBObject("_id", 1));	 
		admindb.command(builder.get());
		
		// pre-split to get parallel writes
		// this http://www.mongodb.org/display/DOCS/Splitting+Chunks says 
		// balancer moving chunks should take 5 minutes ... too long
		// wonder if moveChunk command is faster
		// well we could do it anyway - the jobs that can benefit from it will
		
		// check for user-submitted splitPoints
		String[] splits;
		String splitString = conf.get("mongo.output.split_points","");
	
		// generate our own split points if necessary
		if(splitString.equals("")) {
			long max = (long)Math.pow(93.0, 5.0);
			
			long step = max / shards.size();
			splits = new String[shards.size() -1];
			
			// assume human readable keys
			for(int i=0; i < shards.size() - 1; i++) {
				splits[i] = splitPointForLong(step * (i+1));
			}
		}
		else {
			splits = splitString.split(",");
		}
		
		
		HashMap<String, Object> splitCmd = new HashMap<String, Object>();
		splitCmd.put("split", database + "." + collection);
		splitCmd.put("middle", "");
		
		HashMap<String, Object> moveCmd = new HashMap<String, Object>();
		moveCmd.put("moveChunk", database + "." + collection);
		moveCmd.put("find", "");
		moveCmd.put("to", "");
		
		// do the splitting and migrating
		// we assign chunks to shards in a round-robin manner
		int i=0; 
		for(String split : splits) {
		
			splitCmd.remove("middle");
			splitCmd.put("middle", new BasicDBObject("_id", split));
			
			// create new chunk
			admindb.command(new BasicDBObject(splitCmd));
			
			// move to shard
			moveCmd.remove("find");
			moveCmd.put("find", new BasicDBObject("_id", split));
			moveCmd.put("to", shards.get(i));
			
			admindb.command(new BasicDBObject(moveCmd));
			
			i = (i + 1) % shards.size();	
		}
	}
	
	private String splitPointForLong(long pt) {
		
		String point = "";
		while(pt > 1) {
			long digit = pt % 93;
			point = (char)(digit + 33) + point;
			pt /= 93;
		}
		
		return point;
	}
}

