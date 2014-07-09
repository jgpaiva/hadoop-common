package block.distributor;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

public class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, CompositeKey, RowData>
{
	// static int totalCount=0;
	String filterAttributeId;
	Text sort = new Text();
	String fileName = null;
	CompositeKey compKey = new CompositeKey();
	JobConf conf;

	public void configure(JobConf job)
	{
		filterAttributeId = job.get("groupbyid");
		fileName = job.get("map.input.file");
		String[] arr = fileName.split("/");
		fileName = arr[arr.length - 1].split("\\.")[0];

	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<CompositeKey, RowData> outputCollector,
			Reporter reporter) throws IOException
	{
		// TODO Auto-generated method stub
		List<String> listdata1 = new ArrayList<String>();
		String jsonStr = value.toString();
		JsonParser parser = new JsonParser();
		JsonArray jArray1 = parser.parse(jsonStr).getAsJsonArray();
		String filterVal = "";

		if (jArray1 != null)
		{
			for (int i = 0; i < jArray1.size(); i++)
			{
				listdata1.add(jArray1.get(i).toString());
			}
		}
		if (Integer.parseInt(filterAttributeId) < jArray1.size())
		{
			filterVal = listdata1.get(Integer.parseInt(filterAttributeId));
		}
		else
		{
			System.out.println("Filtered id should be less than array size");
			filterVal = null;
		}

		String keyVal = java.net.InetAddress.getLocalHost().getHostName().replaceAll("[^a-zA-Z0-9]", "");
		// String yourPropertyValue = conf.get("your.property");

		sort.set(keyVal.toString());
		RowData row = new RowData();
		row.setColumns(listdata1);
		row.setFileName(fileName);
		compKey.setHost(keyVal);
		compKey.setGroupValue(filterVal);
		
		outputCollector.collect(compKey, row);// Output has a <Text,Text> format
	}

}
