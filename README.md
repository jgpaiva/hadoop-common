#ShortMap

Working prototype of ShortMap, a system that combines the use of an appropriate data-layout with data indexing tools to improve Hadoop data access speed and significantly shorten the Map phase of jobs.

#ShortMap User Guide

1. Clone the ShortMap source:
`git clone https://github.com/shortmap/shortmap.git`

2. Build ShortMap:
`ant clean
ant`

3. When ShortMap is built, 4 jars are created inside the build folder. Copy these 4 jars into the hadoop-common folder. 

##Configure Hadoop

4. Change the conf/core-site.xml to include total column count (the total number of columns per row in the dataset), index directory (where the index files are stored) and total record count per index (As the index is kept in memory, make sure this amount is not too large). Sample 3 configurations are shown below.
`<property> 
	<name>dfs.block.total.column.count</name> 
	<value>4</value> 
	<description> Total number of columns per row in a dataset 
	</description> 
</property> 
<property> 
	<name>index.dir.loc</name> 
	<value>/home/index</value> 
	<description>The directory where the index is saved 
	</description> 
</property> 
<property> 
	<name>index.record.count</name> 
	<value>50</value> 
	<description>The number of records stored in one index
	</description> 
</property>`

5. Change the conf/mapred-site.xml to have maximum of 1 mapper and reducer per each task node. Sample configuration is shown below.
`<property> 
  	<name>mapreduce.tasktracker.map.tasks.maximum</name> 
  	<value>1</value> 
</property> 
<property> 
  	<name>mapreduce.tasktracker.reduce.tasks.maximum</name> 
  	<value>1</value> 
</property>`

6. Change the conf/hdfs-site.xml to have the replication factor as 1. We assume that there will not be any failures and each node will act as a mapper and a reducer. 
`<property> 
	<name>dfs.replication</name> 
	<value>1</value> 
	<description>Default block replication. </description> 
</property>`

7. Startup the cluster

`./bin/start-all.sh`

##Pre-process data

8. Build BlockDistributor preprocessing job. This is available at hadoop-common/BlockDistributor as a separate project from hadoop source.
`cd BlockDistributor
ant
ant build-jar`

[This project uses a reference to hadoop-core-1.2.1.jar which is generated in step 2.]

9. Run preprocessing job.
`hadoop jar BlockDistributor.jar block.distributor.BlockDistributor -Dmapred.reduce.tasks=<number of nodes>  <node list> <number of columns> <output block size> <input folder> <output folder>`

example:
`hadoop jar BlockDistributor.jar block.distributor.BlockDistributor -Dmapred.reduce.tasks=3  -libjars /home/chathuri/gson-2.2.4.jar /home/gayana/hosts.txt 0 13422222 /hadoop/input /hadoop/opj2 `

[we use gson library to process json array. So users have to specify it with -libjars option]

<number of nodes> = Specifies the number of nodes involved in the system

<node list> = File specifying the machine names of hosts separated by newline. Required to keep one-to-one mapping of mapper to reducer. E.g.:
	machine1.gsd.inesc-id.pt
	machine2.gsd.inesc-id.pt
	machine3.gsd.inesc-id.pt

<number of columns> = This indicates the column number(zero based) that is relevant to the group attribute (which will be used to generate the index). 

<output block size> = Maximum uncompressed block size of a column block (in bytes). 

<input folder> = HDFS input folder path (note: input files should be in compressed (.gz) format)

<output folder> = HDFS output folder path where columnar data will be stored

Note: There should be no index created on the target folders

10. Make sure blocks were created in the output folder and the index created in the path specified in the conf/core-site.xml

11.  Run Hadoop job where the input folder is the output folder of step 9

#Sample hadoop job

In this section we provide a sample hadoop job which uses the configuration presented above.

This job uses column 1 of the input file, filtering records with value “C” in column 0. In order to avoid reading the records of a block after reading records with “C” we have to retrieve the <index column> (column 0 in this case) as well. (To avoid others from map task as shown later. There using String valToConsider = value.toString().split(ShortMapRecordReader.COLUMN_REPLACER)[<Index column>]; we can skip reading the block if valToConsider is not equal to filter val).

##Input file
[C,1,456,789]
[D,2,456,789]
[C,3,456,789]
[B,4,456,789]
[A,5,456,789]

##Main class, TupleCount.java
public class TupleCount extends Configured implements Tool
{

    public static void main(String[] args) throws Exception
    {

        int res = ToolRunner.run(new Configuration(), new TupleCount(), args);
        System.exit(res);

        );
    }

    @Override
    public int run(String[] args) throws Exception
    {
        System.out.println("Output location is " + args[2]);
        JobConf conf = new JobConf(TupleCount.class);
        conf.setJobName("tuplecount");
        conf.setNumReduceTasks(1);
        conf.setOutputKeyClass(Text.class);// Both map and reduce map has <Text,Text> format
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(ShortMapInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(conf, true);
        conf.set("filterValues", "\"C\"");// ,\"D\"
        conf.set("map.input.nodes", "ubuntu:0");
        conf.setIfUnset("columnCount", args[0]);
        conf.setIfUnset("outputloc", args[2]);
        conf.set("relevantAttrs", "0,1");
        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        JobClient.runJob(conf);

        // print file locations
        System.out.println("job completed");
        return 0;
    }
}

##Map class
public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
    private final IntWritable one = new IntWritable(1);
    Text word = new Text();
    String fileName = null;
    JobConf conf = null;

    public void configure(JobConf job)
    {
        conf = job;
        System.out.println("configuring");
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
    throws IOException
    {

        String filterStr = conf.get("filterValues");
        System.out.println("filter is "+filterStr);
        String valToConsider = value.toString().split(ShortMapRecordReader.COLUMN_REPLACER)[0];
        System.out.println("val to condider "+valToConsider);
        if (filterStr.toLowerCase().equals(valToConsider.toLowerCase()))
        {
            word.set(filterStr.toLowerCase().toString());
            outputCollector.collect(word, one);
        } else
        {
            System.out.println("Skipping " + value.toString());
            ShortMapRecordReader.statusId = -1;// stop read if irrelavant
        }
    }
}

##Reducer class
public class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
        System.out.println("inside reducer");
        IntWritable count=new IntWritable();
        int sum=0;
        while (values.hasNext())
        {
            sum++;
            values.next();
        }
        System.out.println("sum is "+sum);
        count.set(sum);
        output.collect(key ,count);
    }
}
