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
import java.net.InetAddress;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Bytes;
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

	static DBObject replSetStatCmd;

	{
		BasicDBObjectBuilder cmdBuilder = new BasicDBObjectBuilder();
		cmdBuilder.add("replSetGetStatus", 1);
		replSetStatCmd = cmdBuilder.get();
	}

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

	private void connect(String location, Configuration conf) throws IOException {


		String[] parts = location.split(":");

		// default port for sharded server
		int port = 27018;
		if(parts.length > 1) 
			port = Integer.parseInt(parts[1]);

		Mongo mongo = new Mongo(parts[0], port);

		// figure out if we can read from this server

		// allow reading from secondaries
		mongo.slaveOk();

		String database = conf.get("mongo.input.database");
		String collection = conf.get("mongo.input.collection");
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
		resultsRead = 0.0f;


	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		// determine if any locations are local 
		ArrayList<String> sortedLocations = new ArrayList<String>();
		ArrayList<String> localLocations = new ArrayList<String>();
		ArrayList<String> remoteLocations = new ArrayList<String>();

		InetAddress local = InetAddress.getLocalHost();


		// see if any of these is local
		for(String loc : split.getLocations()) {
			String[] parts = loc.split(":");

			InetAddress host = InetAddress.getByName(parts[0]);

			if(local.equals(host)) {
				System.err.println(loc + " is local");
				localLocations.add(loc);
			}
			else {
				remoteLocations.add(loc);
			}
		}

		if(localLocations.size() == 0) {
			System.err.println("no input split locations are local");
		}

		// try local first, then remote
		for(String loc : localLocations) {
			sortedLocations.add(loc);
		}
		for(String loc : remoteLocations) {
			sortedLocations.add(loc);
		}


		// read from one of the locations for this shard
		for(String loc : split.getLocations()) {
			try {
				connect(loc, conf);
				return;
			}
			catch (IOException e) {
				// try another location
				System.err.println("trying another location for input split");
			}
		}

		String locs = "";
		for(String loc : split.getLocations()) {
			locs += loc + ", ";
		}

		throw new IOException("could not read from any replicas for shard locations: " + locs);

		// this means the MapReduce will fail - unless maybe the user says not to ...	
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean hadMore = cursor.hasNext();
		if(hadMore) {
			value = cursor.next();
			key = ((ObjectId)value.get("_id")).toString();

			// remove object ID from DBObject
			value.removeField("_id");
			resultsRead++;
		}

		return hadMore;
	}
}
