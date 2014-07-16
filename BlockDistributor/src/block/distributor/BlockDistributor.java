package block.distributor;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.omg.CORBA.INTF_REPOS;

//afr[0] num of columns in dataset,arg[1] groupby column, arg2 input,arg[3] output
public class BlockDistributor extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BlockDistributor(),
				args);
		System.exit(res);

		// job.waitForCompletion(true);
	}

	private static boolean isInteger(String input) {
		try {
			Integer.parseInt(input);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		JobConf conf = new JobConf(getConf(), BlockDistributor.class);
		conf.setJobName("blockdistributor");
		conf.set("jobName", "blockdistributor");
		if (args.length < 5 || !isInteger(args[1]) || !isInteger(args[2])) {
			System.out
					.println("use <arg0> <arg1> <arg2><arg3><arg4><arg5>format when specifying arguments. arg0-host file, arg1-folder to store index,arg2-column to group,arg3-output block size,arg4-ip,arg5-op");
			System.exit(0);
		}
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(CompositeKey.class);
		conf.setPartitionerClass(ActualKeyPartitioner.class);
		conf.setOutputValueGroupingComparator(ActualKeyGroupingComparator.class);
		conf.setOutputKeyComparatorClass(CompositeKeyComparator.class);
		conf.setMapOutputValueClass(RowData.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);

		conf.setOutputFormat(MultipleTextOutputFormat.class);
		TextOutputFormat.setCompressOutput(conf, true);
		TextOutputFormat.setOutputCompressorClass(conf, GzipCodec.class); //
		// to make the outputs to be compressable

		conf.setIfUnset("outputloc", args[4]);
		conf.setIfUnset("groupbyid", args[1]);
		conf.setInt("reduce.block.size", Integer.parseInt(args[2]));
				
		String indexDir=conf.get("index.dir.loc");
		
		if(indexDir==null){
			System.out.println("Please configure the index directory in core-site.xml");
		}
		
		if(!isIndexDirValid(conf,indexDir)){
			System.out.println(indexDir+" already contain some files. Please remove them or change the directory in core-site.xml");
		}

		 //set host list for task trackers
		String hostsSet = getHostsSetfromFile(args[0]);
		conf.setIfUnset("map.input.nodes",hostsSet);

		int totalCount = conf.getInt("dfs.block.total.column.count", 0);
		System.out.println("total count" + totalCount);
		for (int i = 0; i < totalCount; i++) {
			MultipleOutputs.addMultiNamedOutput(conf, "col" + i,
					TextOutputFormat.class, Text.class, Text.class);
		}

		FileInputFormat.setInputPaths(conf, new Path(args[3]));// input file
		FileOutputFormat.setOutputPath(conf, new Path(args[4]));

		JobClient.runJob(conf);

		// write output locations to file
		System.out.println("job completed");

		PrintWriter writer = new PrintWriter("/home/chathuri/sample.txt",
				"UTF-8");
		String fileName = args[4];
		FileSystem fileSystem = FileSystem.get(conf);
		FileStatus[] status = fileSystem.listStatus(new Path(fileName));
		if (status.length > 0) {
			for (int i = 0; i < status.length; i++) {
				if (!status[i].isDir()) {
					writer.println(status[i].getPath());
					BlockLocation[] loc = fileSystem.getFileBlockLocations(
							status[i], 0, status[i].getLen());
					for (int j = 0; j < loc.length; j++) {
						writer.println("----" + loc[j]);
					}
				}
			}
		} else {
			System.out.println("No output file found");
		}
		writer.close();
		return 0;
	}

	/**
	 * @param conf
	 * @return
	 * @throws IOException 
	 */
	private boolean isIndexDirValid(JobConf conf,String fileName) throws IOException
	{
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path(fileName);
		if(!fileSystem.exists(path)){
			return true;
		}
		return false;
	}

	private String getHostsSetfromFile(String hostsFile) {
		// TODO Auto-generated method stub
		String hosts = "";
		try {
			BufferedReader reader = new BufferedReader(
					new FileReader(hostsFile));
			String line = null;
			StringBuilder hostsSet = new StringBuilder();
			int hostId = 0;
			while ((line = reader.readLine()) != null) {
				line = line.replaceAll("[^a-zA-Z0-9]", "");
				hostsSet.append(line).append(":").append(hostId).append(";");
				hostId++;
			}
			hosts = hostsSet.substring(0, hostsSet.length() - 1);
			reader.close();
		} catch (Exception ex) {
			System.out
					.println("Please specify correct hostfile path and name in<args[0]>");
			System.exit(0);
		}
		return hosts;

	}
}
