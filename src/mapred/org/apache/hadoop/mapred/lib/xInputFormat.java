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
import org.apache.hadoop.mapred.xRecordReader;
import org.apache.hadoop.net.NetworkTopology;

/* mgferreira*/

public class xInputFormat extends FileInputFormat<LongWritable, Text> 
implements JobConfigurable {
	public static final Log LOG = LogFactory.getLog(xInputFormat.class);

	private String FIRST_COLUMN_IDENTIFIER = "";
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit genericSplit,
			JobConf job,
			Reporter reporter) 
					throws IOException {
		reporter.setStatus(genericSplit.toString());
		return new xRecordReader(job, (FileSplit) genericSplit);
	}

	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		FileStatus[] files = listStatus(job);

		// Save the number of input files in the job-conf
		job.setLong(NUM_INPUT_FILES, files.length);
		for (FileStatus file: files) {                // check we have valid files
			if (file.isDir()) {
				throw new IOException("Not a file: "+ file.getPath());
			}
		}

		// generate splits only for the first columns of each row group
		ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
		NetworkTopology clusterMap = new NetworkTopology();
		for (FileStatus file: files) {
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

				FileSplit split = new FileSplit(blockId, path, 0, blockSize, splitHosts);		
				splits.add(split);
			}
		}
		return splits.toArray(new FileSplit[splits.size()]);
	}

	public void configure(JobConf conf) {
		FIRST_COLUMN_IDENTIFIER = conf.get("first.column.identifier");
	}
}
