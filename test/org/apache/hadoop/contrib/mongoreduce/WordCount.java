package org.apache.hadoop.contrib.mongoreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;



public class WordCount implements Tool {

	public static class TestMapper extends Mapper<Text, DBObject, Text, IntWritable> {
		
		/**
		 * Mongo document ObjectID's appear as the Text key and are not in the DBObject value
		 */
		public void map(Text key, DBObject value, Context context) throws IOException, InterruptedException {
			
			// read all words from all fields
			for(String k : value.keySet()) {
				
				String words = (String)value.get(k);
			
				HashMap<String, Integer> localWordCount = new HashMap<String, Integer>();
				for(String word : words.split("\\s+")) {
					if(!localWordCount.containsKey(word)) {
						localWordCount.put(word, 1);
					}
					else {
						localWordCount.put(word, localWordCount.get(word) + 1);
					}
				}
				
				for(Entry<String, Integer> entry : localWordCount.entrySet()) {
					context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
				}
			}
		}
	}
		
	public static class TestMongoReducer extends Reducer<Text, IntWritable, Text, DBObject> {
		
		public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
	
			// create a mongo document
			BasicDBObjectBuilder b = new BasicDBObjectBuilder();
			b.add("count", sum);
			
			// key outputted becomes the mongo document's _id
			context.write(word, b.get());
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
				
		Job job = new Job(getConf());

		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCount.TestMapper.class);
		job.setReducerClass(WordCount.TestMongoReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DBObject.class);
		
		//job.setNumReduceTasks(20);
		
		// configure input parameters
		job.setInputFormatClass(MongoInputFormat.class);
		
		MongoInputFormat.setDatabase(job, args[0]);
		MongoInputFormat.setCollection(job, args[1]);
		
		if(args.length > 4)
			MongoInputFormat.setQuery(job, args[4]);
		
		if(args.length > 5)
			MongoInputFormat.setSelect(job, args[5]);
		
		// use to set whether running on primary nodes is allowed - default is false
		// note that this only has an effect if shards are replicated
		// MongoInputFormat.setPrimaryOk(job, true);
		
		// used to test a MapReduce job on a non-sharded, local database
		MongoInputFormat.setSingleTestingMode(job, true);
		
		// configure output parameters
		job.setOutputFormatClass(MongoOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		MongoOutputFormat.setDatabase(job, args[2]);
		MongoOutputFormat.setCollection(job, args[3]);
		
		// whether to drop the output collection before writing - default to false
		
		MongoOutputFormat.dropCollection(job, true);
	
		// the output collection will be pre-split if it is new or if we are dropping it
		// MongoOutputFormat doesn't pre-split an existing collection
		// optionally skip pre-splitting a new (or newly dropped) output collection - default is false
		
		//MongoOutputFormat.skipPreSplitting(job, true);
		
		// optionally pass in a set of split points to use when pre-splitting the output database
		// this helps ensure that results are written evenly across the cluster
		// these could be read from a file
		// this is ignored if it is determined that we aren't pre-splitting
		
		//ArrayList<String> splitPoints;
		// ( ... populate splitPoints here ... )
		//MongoOutputFormat.setSplitPoints(job, points);
		
		// run
		job.waitForCompletion(true);
		
		
		return 0;
	}

	private Configuration conf = null;
	
	@Override
	public Configuration getConf() {
		if(conf == null) 
			conf = new Configuration();
		
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * 
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		if(args.length < 4) {
			System.out.println("Usage: WordCount in_db in_collection out_db out_collection [query statement] [select statement]");
			return;
		}
		
		ToolRunner.run(new WordCount(), args);
	}
}
