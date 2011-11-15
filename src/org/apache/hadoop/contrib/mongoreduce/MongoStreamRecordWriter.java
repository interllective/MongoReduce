/**
 * Copyright 2011 Interllective Inc
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
 * 
 */

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

	//@Override
	public void write(Text key, Text value) {
		
		DBObject objValue = (DBObject)JSON.parse(value.toString());
		
		objValue.put("_id", key.toString());
		
		coll.save(objValue);
	}


	//@Override
	public void close(Reporter reporter) throws IOException {
		mongo.close();
		
	}


}
