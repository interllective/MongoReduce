package org.apache.hadoop.contrib.mongoreduce;
import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class MongoOutputCommitter extends OutputCommitter {

	@Override
	public void abortTask(TaskAttemptContext taskContext) throws IOException {
		// TODO Auto-generated method stub
		// delete everything from this task?

	}

	@Override
	public void cleanupJob(JobContext jobContext) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitTask(TaskAttemptContext taskContext) throws IOException {
		// should we move objects from a temporary spot into the final collection?
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext taskContext)
			throws IOException {

		return false;
	}

	@Override
	public void setupJob(JobContext jobContext) throws IOException {
		// TODO: ensure we have a sharded and split mongo collection?
		// optionally pre-split to get good parallel writes
	}

	@Override
	public void setupTask(TaskAttemptContext taskContext) throws IOException {

	}

}
