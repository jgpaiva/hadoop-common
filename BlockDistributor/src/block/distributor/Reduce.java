package block.distributor;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.LineReader;

public class Reduce extends MapReduceBase implements Reducer<CompositeKey, Writable, Text, Text>
{
	private MultipleOutputs mos;
	JobConf conf;
	String outputDir = "";
	String indexDir="";
	int blockId;
	int maxBlockSize = 134217728;// 128MB u can even use half of this as well.
									// block size.Should read from config

	// int maxBlockSize=100000;
	HashMap<Integer, StringBuilder> currentFileList;
	HashMap<Integer, Long> sizeList;
	HashMap<Integer, String> fileMapping;
	Boolean isNewBlock = true;
	Boolean isFirstTime = true;
	private static final String ESCAPE_CHAR = "\n";
	int escapeCharLength = 0;

	// Start-Index
	TreeMap<String, IndexValueObject> indexMap;
	ArrayList<Long> temporaryOffsetList;
	HashMap<Integer, Long> oldOffsetSizeList;
	String tempIndexData;
	IndexValueObject tempIndexValueObj;
	int indexColumnNumber = 0; 
	private static final String POSTFIX_INDEX_OUTPUT = ".gz";
	StringBuilder indexStringBuilder;
	private static final String SEP_NEWLINE = "\n";
	private static final String SEP_HEAD = "$#Z#$";
	private static final String SEP_SUBSTITUTION = "$%Z%$";
	int substitutionPosition;
	boolean tempIsFirstRecord;
	String tempStringIndexData;
	int maxRecordCountPerBlock;
	String startingIndexKeyofaBlock;
	int globalOutputIndex;
	TreeMap<String, String> masterMap;
	private static final String PREFIX_MASTER_MAP = "master";
	// End-Index
	CompressionCodecFactory compressionCodecs;

	public void configure(JobConf conf)
	{
		outputDir = conf.get("outputloc");
		indexDir="/hadoop/index";

		String confSize = conf.get("reduce.block.size");
		if (confSize != null)
		{
			maxBlockSize = Integer.parseInt(confSize);
		}

		mos = new MultipleOutputs(conf);
		this.conf = conf;
		compressionCodecs = new CompressionCodecFactory(conf);
		indexColumnNumber=conf.getInt("groupbyid", 0);

	}

	@Override
	public void reduce(CompositeKey key, Iterator<Writable> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
	{

		System.out.println("reducer started");
		System.out.println("Block size is " + maxBlockSize);
		// TODO Auto-generated method stub

		Boolean isStartRecord = true;
		String reducerName = java.net.InetAddress.getLocalHost().getHostName().replaceAll("[^a-zA-Z0-9]", "");
		while (values.hasNext())
		{
			RowData row = (RowData) values.next();
			if (isStartRecord)
			{
				initDataStructurs(row, reducerName);
			}
			if (row != null)
			{
				if (isNewBlock(row) || isStartRecord)
				{
					isStartRecord = false;
					clearDataStructure(row);
					addSingleRow(row, reporter, reducerName);
					IndexRecordsHashMapBased(row, reducerName);
					// IndexRecordsStringBased(row, key.getHost());
				} else
				{
					addSingleRow(row, reporter,reducerName);
					IndexRecordsHashMapBased(row, reducerName);
					// IndexRecordsStringBased(row, key.getHost());
				}
			}
		}
		tempIndexValueObj.setEndBlockId(blockId);
		
		System.out.println("Indexing completed. updated");
		//PrintHashMap(indexMap);
		System.out.println(indexStringBuilder.toString());
		writeSerializedOutput(indexMap, false, reducerName);
		UpdateMasterMap(reducerName);
		writeSerializedOutput(masterMap, true, reducerName);
		// writeStringToDisk(reducerName+"_Str"+POSTFIX_INDEX_OUTPUT);
		System.out.println("Serialized indexMap and saved to disk");

		// Test-Index Start
		// readSerializedOutput(reducerName+POSTFIX_INDEX_OUTPUT);
		// Test-Index End

	}

	private void readSerializedOutput(String filePath)
	{
		// TODO Auto-generated method stub
		try
		{
			HashMap<String, IndexValueObject> readMap = new HashMap<String, IndexValueObject>();
			Path pt = new Path(indexDir + "/" + filePath);
			FileSystem fs = FileSystem.get(conf);
			ObjectInputStream br = new ObjectInputStream(fs.open(pt));
			HashMap<String, IndexValueObject> map = (HashMap<String, IndexValueObject>) br.readObject();

			Path blockPath = new Path("/hadoop/op18/col0_ubuntuZblockZ34-r-00000.gz");
			CompressionCodec codec = compressionCodecs.getCodec(blockPath);
			FSDataInputStream fileIn = fs.open(blockPath);
			Decompressor decompressor = CodecPool.getDecompressor(codec);
			if (!(codec instanceof SplittableCompressionCodec))
			{
				CompressionInputStream is = codec.createInputStream(fileIn, decompressor);
				// System.out.println("Available bytes "+is.available());
				is.skip(10736);
				LineReader in = new LineReader(is, conf);
				Text tData = new Text();
				in.readLine(tData);
				System.out.println("DATA RETRIEVERED = " + tData.toString());
				/*
				 * OutputStream out = null; try { String outputURI =
				 * CompressionCodecFactory.removeSuffix(outputDir+"/col0_ubuntuZblockZ34-r-00000.gz",
				 * codec.getDefaultExtension()); out = fs.create(new Path(outputURI + "-uncompressed"));
				 * org.apache.hadoop.io.IOUtils.copyBytes(is, out, conf); System.out.println("Uncompressed"); } catch(Exception
				 * ex) { ex.printStackTrace(); } finally { org.apache.hadoop.io.IOUtils.closeStream(out); IOUtils.closeStream(is);
				 * out.flush(); out.close(); }
				 */
			}

		} catch (Exception ex)
		{
			System.out.println("Read object failed : " + ex.getMessage());
			ex.printStackTrace();
		}
	}

	/*
	 * Test print Index hashmap
	 */
	private void PrintHashMap(TreeMap<String, IndexValueObject> map)
	{
		StringBuilder sb;
		Iterator iterator = map.keySet().iterator();
		while (iterator.hasNext())
		{
			sb = new StringBuilder();
			String key = iterator.next().toString();
			IndexValueObject value = map.get(key);
			sb.append("KEY=" + key);
			sb.append(" START=" + value.getStartBlockId());
			sb.append(" END=" + value.getEndBlockId());
			sb.append(" OFFSET=");
			for (int i = 0; i < value.getOffsetArray().size(); i++)
			{
				sb.append(value.getOffsetArray().get(i).toString() + ",");
			}
			String output = sb.substring(0, sb.length() - 1);
			System.out.println(output);
		}
	}

	private void initDataStructurs(RowData rowData, String reducerHost)
	{
		// TODO Auto-generated method stub
		blockId = 0;
		currentFileList = new HashMap<Integer, StringBuilder>();
		sizeList = new HashMap<Integer, Long>();

		// Start-Index
		indexMap = new TreeMap<String, IndexValueObject>();
		temporaryOffsetList = new ArrayList<Long>();
		oldOffsetSizeList = new HashMap<Integer, Long>();
		tempIndexData = "";
		tempStringIndexData = "";
		tempIndexValueObj = null;
		tempIsFirstRecord = true;
		try
		{
			escapeCharLength = ESCAPE_CHAR.getBytes("UTF-8").length;
		} catch (UnsupportedEncodingException ex)
		{
			// Log the error, set the default byte size of escapse sequence to 1
			escapeCharLength = 1;
		}
		indexStringBuilder = new StringBuilder();
		maxRecordCountPerBlock = 50;
		startingIndexKeyofaBlock = null;
		globalOutputIndex = 0;
		masterMap = new TreeMap<String, String>();
		// End-Index
	}

	private void clearDataStructure(RowData row)
	{
		blockId++;
		for (int i = 0; i < row.getColumns().size(); i++)
		{
			// currentFileList.get(i).setLength(0);
			sizeList.put(i, 0L);
		}
	}

	private void addSingleRow(RowData data, Reporter reporter, String hostName) throws IOException
	{
		String seqName = hostName + "ZblockZ" + blockId;
		for (int i = 0; i < data.getColumns().size(); i++)
		{
			mos.getCollector("col" + i, seqName, reporter).collect(NullWritable.get(), new Text(data.getColumns().get(i)));
			long oldSize = sizeList.get(i);
			oldOffsetSizeList.put(i, oldSize);
			try
			{
				long updatedSize = oldSize + data.getColumns().get(i).getBytes("UTF-8").length + escapeCharLength;
				sizeList.put(i, updatedSize);
			} catch (UnsupportedEncodingException e)
			{
				// TODO Auto-generated catch block
				System.out.println("Exception Thrown while appending a row");
				e.printStackTrace();
			}
		}

	}

	/*
	 * Index the records according to the column configured by user
	 */
	private void IndexRecordsHashMapBased(RowData data, String hostName)
	{
		String indexData = data.getColumns().get(indexColumnNumber);

		if (!indexData.equals(tempIndexData))
		{

			// Set the end block id of the previous record
			if (tempIndexValueObj != null)
			{
				tempIndexValueObj.setEndBlockId(blockId);
			}

			// If exceed the max limit, save the map
			if (indexMap.size() >= maxRecordCountPerBlock)
			{
				writeSerializedOutput(indexMap, false, hostName);
				UpdateMasterMap(hostName);
				indexMap = new TreeMap<String, IndexValueObject>();
				startingIndexKeyofaBlock = null;
				globalOutputIndex++;
			}

			// if save as a new index file set this starting key
			if (startingIndexKeyofaBlock == null)
			{
				startingIndexKeyofaBlock = indexData;
			}

			// Create a new record for the current data
			String indexKey = indexData;
			temporaryOffsetList = new ArrayList<Long>();
			for (int i = 0; i < oldOffsetSizeList.size(); i++)
			{
				temporaryOffsetList.add(oldOffsetSizeList.get(i));
			}
			IndexValueObject indexvalue = new IndexValueObject(temporaryOffsetList, blockId);
			indexMap.put(indexKey, indexvalue);//Index is stored as lowercase
			tempIndexValueObj = indexvalue;
			tempIndexData = indexData;
		}
	}

	private void UpdateMasterMap(String reducerHost)
	{
		String key = startingIndexKeyofaBlock + SEP_HEAD + tempIndexData;
		String value = GenerateOutputFileName(false, reducerHost);
		System.out.println("KEY=" + key + " VALUE=" + value);
		masterMap.put(key, value);
	}

	/*
	 * Index the records according to the column configured by user Below is a sample output record, line ends with newline
	 * character A$#*Z*#$ubuntuZblockZ1$#*Z*#$0$#*Z*#$0$#*Z*#$0$#*Z*#$0$#*Z*#$1$#*Z*#$1
	 */
	private void IndexRecordsStringBased(RowData data, String hostName)
	{
		String indexData = data.getColumns().get(indexColumnNumber);
		if (!indexData.equals(tempStringIndexData))
		{
			tempStringIndexData = indexData;
			indexStringBuilder.append(indexData + SEP_HEAD);
			for (int i = 0; i < oldOffsetSizeList.size(); i++)
			{
				indexStringBuilder.append(oldOffsetSizeList.get(i).toString() + SEP_HEAD);
			}
			indexStringBuilder.append(Integer.toString(blockId) + SEP_HEAD);
			if (!tempIsFirstRecord)
			{
				indexStringBuilder.replace(substitutionPosition, (substitutionPosition + SEP_SUBSTITUTION.length()),
						Integer.toString(blockId));
			}
			substitutionPosition = indexStringBuilder.length();
			indexStringBuilder.append(SEP_SUBSTITUTION + SEP_NEWLINE);
			tempIsFirstRecord = false;
		}
	}

	private Boolean isNewBlock(RowData r)
	{
		for (int i = 0; i < r.getColumns().size(); i++)
		{

			long blockLength;
			try
			{
				if (sizeList.size() != 0)
					blockLength = sizeList.get(i) + r.getColumns().get(i).getBytes("UTF-8").length;
				else
				{
					blockLength = r.getColumns().get(i).getBytes("UTF-8").length;
				}
				if (blockLength > maxBlockSize)
				{
					// save Existing block
					// CreatePreviousFile();
					return true;
				}
			} catch (UnsupportedEncodingException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.out.println("Error occured while getting the UTF-8 length for field" + i);
			}

		}

		return false;
	}

	public void close() throws IOException
	{
		mos.close();
	}

	private String GenerateOutputFileName(boolean isMasterIndex, String reducerHost)
	{
		String fileName = "";
		if (isMasterIndex)
			fileName = reducerHost + PREFIX_MASTER_MAP + POSTFIX_INDEX_OUTPUT;
		else
			fileName = reducerHost + globalOutputIndex + POSTFIX_INDEX_OUTPUT;
		return fileName;
	}

	public void writeSerializedOutput(TreeMap map, boolean isMasterIndex, String reducerHost)
	{
		try
		{
			FileSystem fileSystem = FileSystem.get(conf);
			Path path = new Path(indexDir+"/"+ GenerateOutputFileName(isMasterIndex, reducerHost));
			if (fileSystem.exists(path))
			{
				System.out.println("File " + path + " already exists");
				if (fileSystem.delete(path))
				{
					System.out.println("Deleted the file " + path);
				} else
				{
					System.out.println("Cannot delete file " + path);
					return;
				}
			}
			GZIPOutputStream gz = new GZIPOutputStream(fileSystem.create(path, true));
			ObjectOutputStream br = new ObjectOutputStream(gz);
			br.writeObject(map);
			br.close();
		} catch (Exception ex)
		{
			System.err.println(ex.getMessage());
			ex.printStackTrace();
		}

	}

	public void writeStringToDisk(String outputFileName)
	{
		try
		{
			FileSystem fileSystem = FileSystem.get(conf);
			Path path = new Path(outputDir + "/" + outputFileName);
			if (fileSystem.exists(path))
			{
				System.out.println("File " + path + " already exists");
				if (fileSystem.delete(path))
				{
					System.out.println("Deleted the file " + path);
				} else
				{
					System.out.println("Cannot delete file " + path);
					return;
				}
			}
			GZIPOutputStream gz = new GZIPOutputStream(fileSystem.create(path, true));
			PrintWriter out = new PrintWriter(gz);
			out.write(indexStringBuilder.toString());
			out.close();
		} catch (Exception ex)
		{
			System.err.println(ex.getMessage());
			ex.printStackTrace();
		}

	}

}

/*
 * Indexing related class represents the hashmap value object
 */
class IndexValueObject implements java.io.Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<Long> offsetList;
	private int startBlockId;
	private int endBlockId;

	public IndexValueObject(ArrayList<Long> offsetList, int startBlockId)
	{
		this.offsetList = offsetList;
		this.startBlockId = startBlockId;
		this.endBlockId = -1;
	}

	public ArrayList<Long> getOffsetArray()
	{
		return offsetList;
	}

	public int getStartBlockId()
	{
		return startBlockId;
	}

	public int getEndBlockId()
	{
		return endBlockId;
	}

	public void setEndBlockId(int endBlockId)
	{
		this.endBlockId = endBlockId;
	}

}