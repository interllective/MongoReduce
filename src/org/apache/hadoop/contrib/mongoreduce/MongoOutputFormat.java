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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.mongodb.DBObject;


public class MongoOutputFormat extends OutputFormat<Text, DBObject> {
	
	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {

		return new MongoOutputCommitter();
	}

	@Override
	public RecordWriter<Text, DBObject> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		String database = getDatabase(context);
		String collection = getCollection(context);
		
		return new MongoRecordWriter(database, collection);
	}

	public static String getCollection(TaskAttemptContext context) {
		return context.getConfiguration().get("mongo.output.collection");
	}

	public static String getDatabase(TaskAttemptContext context) {
		return context.getConfiguration().get("mongo.output.database");
	}

	public static void setDatabase(Job job, String db) {
		job.getConfiguration().set("mongo.output.database", db);
	}

	public static void setCollection(Job job, String cl) {
		job.getConfiguration().set("mongo.output.collection", cl);
	}
	
	public static void dropCollection(Job job, boolean drop) {
		job.getConfiguration().setBoolean("mongo.output.drop", drop);
	}
	
	public static void setSplitPoints(Job job, ArrayList<String> points) {
		
		String pointString = "";
		for(String point : points) {
			pointString += point + "\t";
		}
		
		job.getConfiguration().set("mongo.output.split_points", pointString.substring(0, pointString.length()));
	}
	
	public static void skipPreSplitting(Job job, boolean skip) {
		job.getConfiguration().setBoolean("mongo.output.skip_splitting", skip);
	}
}
