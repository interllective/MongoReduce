MongoReduce enables users to run Hadoop MapReduce jobs over data stored in MongoDB via new MongoDB-specific Input and Output formats. Users can choose to read data from a MongoDB collection as input, and write MapReduce results to a MongoDB collection.

MongoReduce is intended to be a faster and more accessible way than the built-in MongoDB MapReduce functionality to perform MapReduce over MongoDB collections. MongoReduce allows jobs to be written as any other Hadoop MapReduce job, which means in Java, or any language supported by Hadoop streaming as opposed to only JavaScript. In addition, MongoReduce leverages all the resilience and optimizations that have been developed within the Hadoop effort. MongoReduce is designed to process entire MongoDB collections in parallel on many machines. 

The MongoReduce input format automatically deserializes the BSON documents stored in MongoDB into DBObjects for manipulation within the Map function. 

The output format can take DBObjects and will write them to the appropriate MongoDB collection. Output collections are automatically sharded and pre-split to gain write parallelism.

MongoReduce has some advantages over running MapReduce over files in HDFS: The input format can be configured to select a portion of the entire input collection by specifying a MongoDB query. This allows the mappers to process only the data of interest, and can take advantage of any indexes that may exist. This important use case was recently highlighted by Jeffrey Dean and Sanjay Ghemawat in Communications of the ACM (http://cacm.acm.org/magazines/2010/1/55744-mapreduce-a-flexible-data-processing-tool/fulltext). Furthermore, using MongoReduce, users can store MapReduce results in a new MongoDB collection, which makes results instantly available for queries. This also enables iterative MapReduce jobs that process the output results of a previous job as input to the next job. 

Drawbacks of running MapReduce over MongoDB include the cost of creating indexes as results are output, and efficiencies lost in reading input that is no longer highly sequential - a MongoDB cursor will likely not read large contiguous blocks from disk like HDFS will, especially when reading the results of a query rather than an entire collection.  


Configuration

MapReduce is a distributed application, and MongoReduce is designed to run over a sharded MongoDB cluster. Any collections read as input should already be sharded. MapReduce TaskTrackers should be run on the each node of the MongoDB cluster that is running a 'mongod --shardsvr' process. In addition, each node should run a local mongos process pointing to the MongoDB config servers. See http://www.mongodb.org/display/DOCS/Sharding for details on setting up a sharded cluster.

In theory, TaskTrackers do not have to run on the same machines and process only the local shard, but this scenario departs from the spirit of MapReduce, which is to perform the same computation on local data on many machines.


Automatic Failure Recovery

While a sharded MongoDB cluster can be run without replica sets, they are necessary to achieve the same resilience of MapReduce jobs running over HDFS. This is because it must be possible to run a Map worker on another copy of the data should one machine fail. In the event of machine failure during a MapReduce job over HDFS, Hadoop will find another HDFS DataNode containing a copy of the data and start another worker process there. In the case of MongoDB, Hadoop will try another MongoDB replica in the set in the event that a machine fails.


Usage

