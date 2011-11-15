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
