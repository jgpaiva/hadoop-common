package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ConstantConfigs;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.ShortMapFileSplit;
import org.apache.hadoop.mapred.ShortMapRecordReader;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShortMapInputFormat extends FileInputFormat<LongWritable, Text>
		implements JobConfigurable
{
	public static final Log LOG = LogFactory.getLog(ShortMapInputFormat.class);

	@Override
	public void configure(JobConf job)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException
	{
		// TODO Auto-generated method stub
		reporter.setStatus(genericSplit.toString());
		return new ShortMapRecordReader(job, (ShortMapFileSplit) genericSplit);
	}

	/**
	 * Create a split for a single row group, define the split row group id and
	 * the split locations
	 * @see org.apache.hadoop.mapred.FileInputFormat#getSplits(JobConf, int)
	 */
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException
	{
		FileStatus[] files = listStatus(job);

		job.setLong(ConstantConfigs.NUM_INPUT_FILES, files.length);
		ArrayList<ShortMapFileSplit> splits = new ArrayList<ShortMapFileSplit>();
		NetworkTopology clusterMap = new NetworkTopology();

		for (int i = 0; i < files.length; i++)
		{
			FileStatus file = files[i];
			// check we have valid files
			if (file.isDir())
			{
				throw new IOException("Not a file: " + file.getPath());
			}

			Path path = file.getPath();
			long length = file.getBlockSize();
			long splitSize = file.getBlockSize();

			long begin = 0;
			FileSystem fs = path.getFileSystem(job);

			if ((length != 0) && isSplitable(fs, path))
			{
				String pathName = path.getName();

				if (!pathName.contains(ConstantConfigs.FIRST_COLUMN_IDENTIFIER_TEXT))
				{
					continue;
				}

				// we create a split only for first column identifier
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file,
						0, length);
				String[] splitHosts = blkLocations[0].getHosts();// As our input
																	// is in zip
																	// format
																	// file
																	// stores in
																	// one node
				// String[] splitHosts = getSplitHosts(blkLocations, 0,
				// splitSize, clusterMap);

				// hadoop/attempt_201406251439_0001_r_000000_0/col10_lesbosZblockZ2-r-00002.gz";
				// lesbosZblockZ2

				String tmpName = pathName
						.substring(pathName.lastIndexOf('_') + 1);
				String rowGroupName = tmpName
						.substring(0, tmpName.indexOf('-'));
				int rowGroupId = 0;

				try
				{
					rowGroupId = Integer.parseInt(rowGroupName
							.split(ConstantConfigs.SPECIFIC_PREFIX)[1]);
				} catch (Exception ex)
				{
					LOG.info("Error while setting row group id");
					ex.printStackTrace();
				}

				ShortMapFileSplit mapSplit = new ShortMapFileSplit(rowGroupId,
						rowGroupName, path, begin, length, splitHosts);
				splits.add(mapSplit);
			}

		}
		return splits.toArray(new ShortMapFileSplit[splits.size()]);
	}
}
