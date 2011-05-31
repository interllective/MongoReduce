package org.apache.hadoop.contrib.mongoreduce;
import java.io.IOException;

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

	private static String getCollection(TaskAttemptContext context) {
		return context.getConfiguration().get("mongo.output.collection");
	}

	private static String getDatabase(TaskAttemptContext context) {
		return context.getConfiguration().get("mongo.output.database");
	}

	public static void setDatabase(Job job, String db) {
		job.getConfiguration().set("mongo.output.database", db);
	}

	public static void setCollection(Job job, String cl) {
		job.getConfiguration().set("mongo.output.collection", cl);
	}

}
