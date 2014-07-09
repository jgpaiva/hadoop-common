package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.mortbay.log.Log;

public class ShortMapRecordReader implements RecordReader<LongWritable, Text>
{

	private static final org.apache.commons.logging.Log LOG = LogFactory
			.getLog(ShortMapRecordReader.class.getName());
	String FIRST_COLUMN_IDENTIFIER;
	private int currentRowGroupIndex;
	private ShortMapFileSplit split;
	private Configuration job;
	private String columnsOffsets;
	public static long statusId = -1;// -1 block is not relavant;0 should start from 0th
						// position;>0 read from offset
    private String COLUMN_SEPERATOR=";$;#;";
    public static final String COLUMN_REPLACER=";\\$\\;#;";
	private CompressionCodecFactory compressionCodecs = null;
	private CompressionCodec codec;
	private Decompressor decompressor;
	private Seekable filePosition;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	int maxLineLength;

	private ArrayList<FSDataInputStream> array2inputStreams = new ArrayList<FSDataInputStream>();
	private ArrayList<LineReader> inN = new ArrayList<LineReader>();
	private ArrayList<Seekable> filePositionN = new ArrayList<Seekable>();
	private ArrayList<Long> posN = new ArrayList<Long>();
	private ArrayList<Long> startN = new ArrayList<Long>();
	private ArrayList<CompressionCodec> codecN = new ArrayList<CompressionCodec>();

	/**
	 * A class that provides a line reader from an input stream.
	 * 
	 * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
	 */
	@Deprecated
	public static class LineReader extends org.apache.hadoop.util.LineReader
	{
		LineReader(InputStream in)
		{
			super(in);
		}

		LineReader(InputStream in, int bufferSize)
		{
			super(in, bufferSize);
		}

		public LineReader(InputStream in, Configuration conf)
				throws IOException
		{
			super(in, conf);
		}
	}

	public ShortMapRecordReader(Configuration job, ShortMapFileSplit split)
			throws IOException
	{
		this.currentRowGroupIndex = 0;
		this.job = job;
		this.split = split;
		FIRST_COLUMN_IDENTIFIER = job
				.get(ConstantConfigs.FIRST_COLUMN_IDENTIFIER_TEXT);
		openRowGroup();
	}

	/**
	 * Combine the filtered columns to form a single line to be sent to the next
	 * method. Whether the split is relevant or not is checked by getting the
	 * offset from index
	 * 
	 * @throws IOException
	 */
	private void openRowGroup() throws IOException
	{
		LOG.info("inside create row group");
		currentRowGroupIndex = split.getRowGroupId();
		String rowGroupName = split.getRowGroupName();
		
		LOG.info("row group index "+currentRowGroupIndex+" name "+rowGroupName);
		// As we add the first column path
		// to index this is first column
		// path
		array2inputStreams.clear();
		inN.clear();
		posN.clear();
		ArrayList<Path> pathsToRelavantAttr = getRelavantAttrPaths(
				split.getPath(), rowGroupName);

		System.out.println("Path count is " + pathsToRelavantAttr.size());

		Path firstFile = pathsToRelavantAttr.remove(0);

		LOG.info("path count after ist remove " + pathsToRelavantAttr.size());

		//
		String indexNode =split.getRowGroupName().split(ConstantConfigs.SPECIFIC_PREFIX)[0];//Relevant index file name for the block
		
		LOG.info("index name is "+indexNode);
		String status = "-1";// 0;0;0
		try
		{
			status = MapTask.relevantRowGroup(currentRowGroupIndex, indexNode,
					job);
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		ArrayList<Long> offsetList = new ArrayList<Long>();

		LOG.info("Status by index is " + status);

		String[] valSet = status.split(";");
		statusId = Integer.parseInt(valSet[0]);

		if (statusId != -1) //-1 if block is irrelevant
		{
			if (statusId > 0)
			{
				for (int i = 1; i < valSet.length; i++)
					offsetList.add(Long.parseLong(valSet[i]));
			} else
			{
				for (int i = 0; i < pathsToRelavantAttr.size(); i++)
				{
					offsetList.add(0L);// All files should be started from
										// started
										// position
				}
			}

			// LOG.info("other offset rec count " + offsetList.size());

			FileSystem fs = firstFile.getFileSystem(job);
			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

			start = split.getStart();
			end = start + split.getLength();
			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.getCodec(firstFile);
			FSDataInputStream fileIn = fs.open(firstFile);
			int i = 0;
			for (Path path : pathsToRelavantAttr)
			{
				array2inputStreams.add(i, fs.open(path));
				startN.add(i, new Long(0));
				CompressionCodecFactory compressionCodecsTmp = new CompressionCodecFactory(
						job);
				codecN.add(i, compressionCodecsTmp.getCodec(path));
				i++;
			}

			if (isCompressedInput())
			{
				decompressor = CodecPool.getDecompressor(codec);
				if (!(codec instanceof SplittableCompressionCodec))
				{
					CompressionInputStream is = codec.createInputStream(fileIn,
							decompressor);
					if (statusId != 0)
					{
						is.skip(statusId);
					}
					in = new LineReader(is, job);
					filePosition = fileIn;

					i = 0;
					for (FSDataInputStream fileInAttr : array2inputStreams)
					{
						CompressionCodec codecTmp = codecN.get(i);
						Decompressor decompressorTmp = CodecPool
								.getDecompressor(codecTmp);
						CompressionInputStream isN = codecTmp
								.createInputStream(fileInAttr, decompressorTmp);
						if (offsetList.get(i) != 0)
						{// Offset to be correct,
							// users should specify
							// the attribute numbers
							// in ascending order
							isN.skip(offsetList.get(i));
						}
						inN.add(i, new LineReader(isN, job));
						filePositionN.add(i, fileInAttr);
						i++;
					}
				}
			} else
			{
				fileIn.seek(statusId);
				in = new LineReader(fileIn, job);
				filePosition = fileIn;

				i = 0;
				for (FSDataInputStream fileInN : array2inputStreams)
				{
					fileInN.seek(offsetList.get(i));
					inN.add(i, new LineReader(fileInN, job));
					filePositionN.add(i, fileInN);
					i++;
				}
			}
			// If this is not the first split, we always throw away first record
			// because we always (except the last split) read one extra line in
			// next() method.
			if (start != 0)
			{
				start += in.readLine(new Text(), 0, maxBytesToConsume(start));

				i = 0;
				for (LineReader reader : inN)
				{
					long startn = startN.remove(i).longValue();
					startn += reader.readLine(new Text(), 0,
							maxBytesToConsume(startn));
					startN.add(i, new Long(startn));
					i++;
				}
			}
			this.pos = start;
			i = 0;
			for (Long startn : startN)
			{
				posN.add(i, startn);
				i++;
			}
		}

	}

	/**
	 * @param rowGroupName
	 *            Return the list of relevant paths. The first column should
	 *            always be specified by the user
	 */
	private ArrayList<Path> getRelavantAttrPaths(Path file, String rowGroupName)
	{
		// TODO Auto-generated method stub
		ArrayList<Path> relevantPaths = new ArrayList<Path>();
		String fileName = file.getName();
		String firstFilePart = ConstantConfigs.COLUMN_PREFIX + 0 + "_" + rowGroupName;
		String relevantAttrStr = job.get(ConstantConfigs.RELEVANT_ATTR_CONFIG);

		String[] relevantAttrs = relevantAttrStr.split(",");
		Path filePath = file.getParent();

		for (String attr : relevantAttrs)
		{
			String newFilePart = ConstantConfigs.COLUMN_PREFIX + attr + "_"
					+ rowGroupName;
			String newFileName = fileName.replace(firstFilePart, newFilePart);

			// System.out.println("Getting relavant path for " + newFileName);
			Path path = new Path(filePath, newFileName);
			if (attr.equals("0"))
			{
				Log.info("Is in 0 th pos");
				relevantPaths.add(0, path);
			} else
			{
				relevantPaths.add(path);
			}
		}
		return relevantPaths;
	}

	private boolean isCompressedInput()
	{
		return (codec != null);
	}

	private int maxBytesToConsume(long pos)
	{
		return isCompressedInput() ? Integer.MAX_VALUE : (int) Math.min(
				Integer.MAX_VALUE, end - pos);
	}

	private long getFilePosition() throws IOException
	{
		long retVal;
		if (isCompressedInput() && null != filePosition)
		{
			retVal = filePosition.getPos();
		} else
		{
			retVal = pos;
		}
		return retVal;
	}

	public ShortMapRecordReader(InputStream in, long offset, long endOffset,
			int maxLineLength)
	{
		this.maxLineLength = maxLineLength;
		this.in = new LineReader(in);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
		this.filePosition = null;
	}

	public ShortMapRecordReader(InputStream in, long offset, long endOffset,
			Configuration job) throws IOException
	{
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		this.in = new LineReader(in, job);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
		this.filePosition = null;
	}

	@Override
	/**
	 * Edited original behavior to combine lines from selected attributes and pass that to the map function
	 */
	public boolean next(LongWritable key, Text value) throws IOException
	{
		while (getFilePosition() <= end)
		{
			if (stop())
			{// we should process only if there is a record to
				// process
				return false;
			}
			key.set(pos);
			int newSize = in.readLine(value, maxLineLength,
					Math.max(maxBytesToConsume(pos), maxLineLength));
			// Text accumulator = new Text(value.toString());
			if (newSize == 0)
			{
				return false;
			}
			int i = 0;
			Long posLength = 0L;
			int columnSize = 0;
			Text columnLine;

			for (LineReader in1 : inN)
			{
				columnLine = new Text();
				posLength = posN.get(i);
				columnSize = in1.readLine(columnLine, maxLineLength,
						Math.max(maxBytesToConsume(posLength), maxLineLength));

				// LOG.info("Line to add is " + columnLine);
				value.set(value.toString() + ";$;#;" + columnLine.toString());
				if (columnSize != 0)
				{
					posLength += columnSize;
					posN.remove(i);
					posN.add(i, new Long(posLength));
				}
				i++;

				pos += newSize;
			}
			if (newSize < maxLineLength)
			{
				// LOG.info("accumulator str is " + value.toString());
				return true;
			}
			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		return false;
	}

	/**
	 * @return
	 */
	private boolean stop()
	{
		// TODO Auto-generated method stub
		return statusId == -1 ? true : false;
	}

	@Override
	public LongWritable createKey()
	{
		// TODO Auto-generated method stub
		return new LongWritable();
	}

	@Override
	public Text createValue()
	{
		// TODO Auto-generated method stub
		return new Text();
	}

	@Override
	public long getPos() throws IOException
	{
		// TODO Auto-generated method stub
		return pos;
	}

	@Override
	public void close() throws IOException
	{
		try
		{
			if (in != null)
				in.close();

			for (LineReader in : inN)
			{
				try
				{
					if (in != null)
					{
						in.close();
					}
				} finally
				{
					if (decompressor != null)
					{
						CodecPool.returnDecompressor(decompressor);
					}
				}
			}
		} finally
		{
			if (decompressor != null)
			{
				CodecPool.returnDecompressor(decompressor);
			}
		}

	}

	@Override
	public float getProgress() throws IOException
	{
		if (start == end)
		{
			return 0.0f;
		} else
		{
			return Math.min(1.0f, (getFilePosition() - start)
					/ (float) (end - start));
		}
	}

}
