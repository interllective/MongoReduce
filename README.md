MongoReduce enables users to run Hadoop MapReduce jobs over data stored in MongoDB via new MongoDB-specific Input and Output formats. Users can choose to read data from a MongoDB collection as input, and write MapReduce results to a MongoDB collection.

MongoReduce is intended for those with a large amount of data stored in MongoDB that don't want to maintain two separate systems - one for query and one for bulk analysis. Traditional data warehousing involves copying data from an online query system to another database for aggregation. MongoReduce lets users keep their data online for query while running MapReduce on secondary replicas.

MongoReduce is designed to be a faster and more accessible way than the built-in MongoDB MapReduce functionality to perform MapReduce over MongoDB collections. MongoReduce allows jobs to be written as any other Hadoop MapReduce job, which means in Java, or any language supported by Hadoop streaming as opposed to only JavaScript. In addition, MongoReduce leverages all the resilience and optimizations that have been developed within the Hadoop effort. MongoReduce is designed to process entire MongoDB collections in parallel on many machines. 


Input Format
------------

The MongoReduce input format automatically deserializes the BSON documents stored in MongoDB into DBObjects for manipulation within the Map function. 

Output Format
-------------

The output format can take DBObjects and will write them to the appropriate MongoDB collection. Output collections are automatically sharded and pre-split to gain write parallelism.

Comparison to HDFS
------------------

MongoReduce has some advantages over running MapReduce over files in HDFS: The input format can be configured to select a portion of the entire input collection by specifying a MongoDB query. This allows the mappers to process only the data of interest, and can take advantage of any indexes that may exist. This important use case was recently highlighted by Jeffrey Dean and Sanjay Ghemawat in [Communications of the ACM](http://cacm.acm.org/magazines/2010/1/55744-mapreduce-a-flexible-data-processing-tool/fulltext "MapReduce"). Furthermore, using MongoReduce, users can store MapReduce results in a new MongoDB collection, which makes results instantly available for queries. This also enables iterative MapReduce jobs that process the output results of a previous job as input to the next job. 

Drawbacks of running MapReduce over MongoDB include the cost of creating indexes as results are output, and efficiencies lost in reading input that is no longer highly sequential - a MongoDB cursor will likely not read large contiguous blocks from disk like HDFS will, especially when reading the results of a query rather than an entire collection.  


Configuration
-------------

MapReduce is a distributed application, and MongoReduce is designed to run over a sharded MongoDB cluster. Any collections read as input should already be sharded. 

1. MapReduce TaskTrackers should be run on the each node of the MongoDB cluster that is running a 'mongod --shardsvr' process. 
2. Each node should run a local mongos process pointing to the MongoDB config servers. See [MongoDB Sharding Documentation](http://www.mongodb.org/display/DOCS/Sharding "Sharding") for details on setting up a sharded cluster.
3. An instance of HDFS should also be available, even when reading from and writing to MongoDB, since HDFS is used to store intermediate results. The HDFS DataNode process should be run on the same nodes as TaskTracker and mongod processes. 
4. Both the mongoreduce.jar file and the mongo-2.5.3.jar must be copied to the $HADOOP_HOME/lib directory of each node of the cluster.

So each node of a mongo cluster is running mongod, mongos, TaskTracker, and DataNode processes. 

In theory, TaskTrackers do not have to run on the same machines and process only the local shard, but this scenario departs from the spirit of MapReduce, which is to perform the same computation on local data on many machines.

MongoReduce has been tested on clusters running Hadoop 0.20.2 and with the MongoDB java driver version 2.5.3


Automatic Failure Recovery
--------------------------

While a sharded MongoDB cluster can be run without replica sets, they are necessary to achieve the same resilience of MapReduce jobs running over HDFS. This is because it must be possible to run a Map worker on another copy of the data should one machine fail. In the event of machine failure during a MapReduce job over HDFS, Hadoop will find another HDFS DataNode containing a copy of the data and start another worker process there. In the case of MongoDB, Hadoop will try another MongoDB replica in the set in the event that a machine fails. MongoReduce automatically discovers replica sets, no configuration is necessary to use this feature.


Usage Example - WordCount
-------------------------

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
			
			// Mongo document ObjectID's appear as the Text key and are not in the DBObject value
			
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
	
		public static void main(String[] args) throws Exception {
			
			if(args.length < 4) {
				System.out.println("Usage: WordCount in_db in_collection out_db out_collection [query statement] [select statement]");
				return;
			}
			
			ToolRunner.run(new WordCount(), args);
		}
	}




Map-Only Jobs
-------------

