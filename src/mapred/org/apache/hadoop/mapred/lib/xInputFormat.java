/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.xIndexUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.xFileSplit;
import org.apache.hadoop.mapred.xRecordReader;
import org.apache.hadoop.net.NetworkTopology;

/* mgferreira*/

public class xInputFormat extends FileInputFormat<LongWritable, Text> 
implements JobConfigurable {
	public static final Log LOG = LogFactory.getLog(xInputFormat.class);

	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

	private String FIRST_COLUMN_IDENTIFIER = "";
	private int nNodes = 0;

	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit genericSplit,
			JobConf job,
			Reporter reporter) 
					throws IOException {
		reporter.setStatus(genericSplit.toString());
		return new xRecordReader(job, (xFileSplit) genericSplit);
	}

	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		FileStatus[] files = listStatus(job);

		// Save the number of input files in the job-conf
		job.setLong(NUM_INPUT_FILES, files.length);
		FIRST_COLUMN_IDENTIFIER = job.get("first.column.identifier");
		nNodes = job.getInt("number.of.nodes", 1);

		for(int i = 0; i < files.length; i++) {
			FileStatus file = files[i];
			// check we have valid files
			if (file.isDir()) {
				throw new IOException("Not a file: "+ file.getPath());
			}
		}

		boolean equalSplits = job.getBoolean("equal.splits", false);
		if(equalSplits) {
			int numberOfBlocksPerSplit = job.getInt("blocks.per.split", 1);
			return buildEqualSplits(job, files, numSplits, numberOfBlocksPerSplit);
		} 
		else {
			String sizeOfSplits = job.get("blocks.per.split");
			List<Integer> splitList = buildSplitList(sizeOfSplits);
			return buildDifferentSplits(job, files, numSplits, splitList);
		}
	}

	private InputSplit[] buildEqualSplits(JobConf job, FileStatus[] files, int numSplits, int numberOfBlocksPerSplit) throws IOException {
		ArrayList<xFileSplit> splits = new ArrayList<xFileSplit>(numSplits);
		NetworkTopology clusterMap = new NetworkTopology();
		ArrayList<Path> paths = null;
		ArrayList<Long> blocksIds = null;

		int j = 0;  
		for(int i = 0; i < files.length; i++) {
			FileStatus file = files[i];
			Path path = file.getPath();
			long length = file.getLen();
			long blockSize = file.getBlockSize();
			long splitSize = file.getLen();
			FileSystem fs = path.getFileSystem(job);

			if ((length != 0) && isSplitable(fs, path)) {
				String fileName = path.getName();
				/* since we want to create a split per each row group 
				 * we will create a split per each first column of each row group */
				if(!fileName.contains(FIRST_COLUMN_IDENTIFIER)) {
					continue;
				}
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
				String[] splitHosts = getSplitHosts(blkLocations, 0, splitSize, clusterMap);
				long blockId = blkLocations[0].getBlockId();

				if ((j % numberOfBlocksPerSplit) == 0) {
					paths = new ArrayList<Path>();
					blocksIds = new ArrayList<Long>();
					xFileSplit split = new xFileSplit(blocksIds, numberOfBlocksPerSplit, paths, 0, blockSize, splitHosts);
					splits.add(split);
				}
				paths.add(path);
				blocksIds.add(new Long(blockId));
				j++;
			}
		}
		return splits.toArray(new xFileSplit[splits.size()]);
	}

	private InputSplit[] buildDifferentSplits(JobConf job, FileStatus[] files, int numSplits, List<Integer> splitList)
			throws IOException {
		// generate splits only for the first columns of each row group
		ArrayList<xFileSplit> splits = new ArrayList<xFileSplit>(numSplits);
		NetworkTopology clusterMap = new NetworkTopology();
		ArrayList<Path> paths = null;
		ArrayList<Long> blocksIds = null;
		int i = 0;
		for(Integer NblocksInThisSplit : splitList) {
			int nFilesToThisSplit = NblocksInThisSplit.intValue();
			for(int k = 0; i < files.length && k < nFilesToThisSplit; i++, k++) {
				FileStatus file = files[i];
				Path path = file.getPath();
				long length = file.getLen();
				long blockSize = file.getBlockSize();
				long splitSize = file.getLen();
				FileSystem fs = path.getFileSystem(job);

				if ((length != 0) && isSplitable(fs, path)) {
					String fileName = path.getName();
					/* since we want to create a split per each row group 
					 * we will create a split per each first column of each row group */
					if(!fileName.contains(FIRST_COLUMN_IDENTIFIER)) {
						continue;
					}

					BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);

					String[] splitHosts = getSplitHosts(blkLocations, 0, splitSize, clusterMap);
					long blockId = blkLocations[0].getBlockId();

					if (k == 0) {
						paths = new ArrayList<Path>();
						blocksIds = new ArrayList<Long>();
						xFileSplit split = new xFileSplit(blocksIds, NblocksInThisSplit, paths, 0, blockSize, splitHosts);
						splits.add(split);
					}
					paths.add(path);
					blocksIds.add(new Long(blockId));
				}
			}
		}
		return splits.toArray(new xFileSplit[splits.size()]);
	}

	public void configure(JobConf conf) {}

	private List<Integer> buildSplitList(String splitsString) {
		//<k, v> : v splits with k blocks
		List<Integer> splitList = new ArrayList<Integer>();
		splitList = new ArrayList<Integer>();

		String[] splits = splitsString.split(":");
		for(int n = 0; n < nNodes; n++) {
			for (String splitString : splits) {
				int numberOfSplitsPerNode = (Integer.parseInt(splitString.split("-")[0])/nNodes);
				for(int m = 0; m < numberOfSplitsPerNode; n++) {
					splitList.add(new Integer(Integer.parseInt(splitString.split("-")[1])));
				}
			}
		}
		System.out.println(splitList);
		return splitList;
	} 
}
