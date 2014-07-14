/**
 * 
 */
package org.apache.hadoop.util;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.IndexValueObject;

/**
 * @author chathuri
 * 
 */
public class ShortMapIndex
{
	static String indexDir;
	public static final String fileMapFileName = "master.gz";
	//static TreeMap<String, IndexValueObject> index;
	static TreeMap<String, IndexValueObject> index;
	static TreeMap<String, String> filterMap;// keep key to file mapping for each user specified key set
	static TreeMap<String, String> fileMap;// key range to file map;load from file system
	static String keyPairSeperator = "\\$#Z#\\$";
	static String indexNameInMemory = null;
	static String oldFilterValues;
	static String oldSelectedAttrs;
	static final String OUTPUT_LOC_CONFIG="outputloc";
	static final String RELEVANT_ATTR_CONFIG="relevantAttrs";
	static String FILTER_VALUES_CONFIG="filterValues";
	static String INDEX_DIR_LOCATION="index.dir.loc";
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(ShortMapIndex.class.getName());

	public static String getOffset(int rowGroupId, String indexNode, Configuration job)
	{
		try
		{
			if (filterMap == null || isNotSameJob(job))
			{
				setRelevantFilesMapToLoad(rowGroupId, indexNode, job); // set the new filtermap

			}
			if (filterMap == null || filterMap.size() == 0)
			{
				return "0"; // no filter attributes specified
			} else
			{
				Iterator<String> it = filterMap.keySet().iterator();
			    LOG.info("filter map value count is "+filterMap.values().size());
				while (it.hasNext())
				{
					String filter = it.next();
					String fileName = filterMap.get(filter);
					LOG.info("file name is "+fileName);

					if (index == null ||indexNameInMemory==null || !fileName.equals(indexNameInMemory))
					{
						// load index
						String outputDir = job.get(OUTPUT_LOC_CONFIG);
						String indexName=indexDir +"/"+ fileName;
						LOG.info("Index name going to load is "+indexName);
						Path pt = new Path(indexName);
						
						LOG.info("Index file loading is "+pt.getName());
						FileSystem fs = FileSystem.get(job);
						
						try{
						HackedObjectInputStream br = new HackedObjectInputStream(new GZIPInputStream(fs.open(pt)));
						index = (TreeMap<String, IndexValueObject>) br.readObject();
						indexNameInMemory = fileName;
						
						LOG.info("Index size is "+index.size());
						PrintHashMap(index);
						}
						catch(Exception ex){
						LOG.info("index specific error "+ex.getMessage());
						ex.printStackTrace();
						}
					
					}

					if (index.containsKey(filter))
					{
						LOG.info("Index contains the key " + filter);
						IndexValueObject indexVal = index.get(filter);
						String offsetAttrs = job.get(RELEVANT_ATTR_CONFIG);
						return generateStrForOffset(offsetAttrs, indexVal, rowGroupId);

					}
				}
				return "-1";// any key not exist in any related index so it is irrelevant
			}

		}

		catch (Exception ex)
		{
			LOG.info("Error occured while loading index " + ex.getMessage());
			//ex.printStackTrace();
			return ex.getMessage();
		}
	}

	
	private static void PrintHashMap(TreeMap<String, IndexValueObject> map)
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
			LOG.info(output);
		}
	}

	
	/**
	 * @param offsetAttrs
	 * @param indexVal
	 * @return
	 */
	private static String generateStrForOffset(String offsetAttrs, IndexValueObject indexVal, int rowGroupId)
	{
		try{
		int startBlockId = indexVal.getStartBlockId();
		int endBlockId = indexVal.getEndBlockId();
		
		LOG.info("End block id is "+endBlockId+" row group id is "+rowGroupId+" start block is "+startBlockId);
		
		StringBuilder offsetStr = new StringBuilder();
		if (rowGroupId == startBlockId)// read starting from offset
		{
			int columnId;
			long offsetVal;
			String[] offsetAttrArray = offsetAttrs.split(",");
			for (String attrColumn : offsetAttrArray)
			{
				columnId = Integer.parseInt(attrColumn);
				offsetVal = indexVal.getOffsetArray().get(columnId);// read offset for relevant column
				offsetStr.append(offsetVal + ";");
			}
			offsetStr.replace(offsetStr.length() - 1, offsetStr.length(), "");
			LOG.info("Offset is " + offsetStr.toString());
			return offsetStr.toString();
		} else
		{
			if (isRowGroupIdInRange(rowGroupId, startBlockId, endBlockId))
			{
				LOG.info("read from first line");
				return "0";// this is not the first block. So key appears starting from first line
			} else
			{
				LOG.info("Block is irrelevant");
				return "-1";// Eventhough key is in index, the requested block is not relavant
			}
		}
		}
		catch(Exception ex){
			LOG.info("Error occured at generateStrForOffset");
			return "-1";
		}

	}

	/**
	 * @param rowGroupId
	 * @param startBlockId
	 * @param endBlockId
	 * @return
	 */
	private static boolean isRowGroupIdInRange(int rowGroupId, int startBlockId, int endBlockId)
	{
		try{
		// TODO Auto-generated method stub
		if (rowGroupId >= startBlockId && rowGroupId <= endBlockId)
		{
			return true;
		}
		return false;
		}
		catch(Exception ex){
		LOG.info("Error occured at isRowGroupIdInRange"+ex.getMessage());
		return false;
		}
	}

	/**
	 * @param job
	 * @return
	 */
	private static boolean isNotSameJob(Configuration job)
	{
		try{
		// TODO Auto-generated method stub
		String newFilterValues = job.get(FILTER_VALUES_CONFIG);
		String newSelectedAttributes = job.get(RELEVANT_ATTR_CONFIG);

		if (oldFilterValues == null || oldSelectedAttrs == null || !oldFilterValues.equals(newFilterValues)
				|| !oldSelectedAttrs.equals(newSelectedAttributes))
		{
			oldFilterValues = newFilterValues;
			oldSelectedAttrs = newSelectedAttributes;
			return true;
		}
		return false;
		}
		catch(Exception ex){
			LOG.info("Error occured at isNotSameJob "+ex.getMessage());
			return false;
		}

	}

	/**
	 * @param indexNode
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static void setRelevantFilesMapToLoad(int rowGroupId, String indexNode, Configuration job)
	{
		try{
		if (fileMap == null || isNotSameJob(job))
		{
			indexDir=job.get(INDEX_DIR_LOCATION);
			String outputDir = job.get(OUTPUT_LOC_CONFIG);
			Path pt = new Path(indexDir+"/" +indexNode+ fileMapFileName);
			LOG.info("Index name to load is " + pt.getName());
			FileSystem fs = FileSystem.get(job);
			ObjectInputStream br = new ObjectInputStream(new GZIPInputStream(fs.open(pt)));
			fileMap = (TreeMap<String,String>) br.readObject();

		}
		LOG.info("index file count by filemap " + fileMap.size());

		String filters = job.get(FILTER_VALUES_CONFIG);
		LOG.info("filter values are " + filters);

		if (filters != null)
		{
			String[] filtersByAttr = filters.split(",");

			filterMap = new TreeMap<String, String>();
			for (String filterVal : filtersByAttr)
			{// iterate through each filter val to load all files
				String fileName = getFileNameForFilterValue(filterVal);
				LOG.info("Attr name " + filterVal + " file name " + fileName);
				filterMap.put(filterVal, fileName);
			}
		}
		}
		catch(Exception ex){
			LOG.info("Error at setRelevantFilesMapToLoad "+ex.getMessage());
		}
	}

	/**
	 * @param filteredAttr
	 * @param fileMap2
	 * @return
	 */
	private static String getFileNameForFilterValue(String filteredAttr)
	{
		try{
		LOG.info("Filtered Attr is "+filteredAttr);
		Iterator<String> iterator = fileMap.keySet().iterator();
		while (iterator.hasNext())
		{
			String keyRange = iterator.next();
			LOG.info("key range is "+keyRange);
			if (rowGroupIsInRange(filteredAttr, keyRange))
			{
				String indexFileName = fileMap.get(keyRange);
				return indexFileName;
			}
		}
		return null;
		}
		catch(Exception ex){
			LOG.info("errror at getFileNameForFilterValue "+ex.getMessage());
			return null;
		}
	}

	/**
	 * @param rowGroupId
	 * @param rowGroupRange
	 * @return
	 */
	private static boolean rowGroupIsInRange(String filteredAttr, String rowGroupRange)
	{
		try{
		String[] pairs = rowGroupRange.split(keyPairSeperator);
        LOG.info("pair 0 "+pairs[0]);
        LOG.info("pair 1"+pairs[1]);
		
		
		int isLargerToLowerBound = pairs[0].compareTo(filteredAttr);
		int isLowertoUpperBound = pairs[1].compareTo(filteredAttr);
		
		LOG.info("lower bound "+isLargerToLowerBound+"upper  "+isLowertoUpperBound);
		
		if (isLargerToLowerBound <= 0 && isLowertoUpperBound >= 0)	
			
		{
			LOG.info("is in range");
			return true;
		}
		LOG.info("No match");
		return false;
		}
		catch(Exception ex){
			LOG.info("error at rowGroupIsInRange "+ex.getMessage());
			return false;
		}
	}

}

