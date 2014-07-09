package block.distributor;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class ActualKeyPartitioner implements Partitioner<CompositeKey, RowData>
{
	HashPartitioner<Text, RowData> hashPartitioner = new HashPartitioner<Text, RowData>();
	Text newKey = new Text();
	HashMap<String, Integer> mappingList = new HashMap<String, Integer>();
	@Override
	public int getPartition(CompositeKey key, RowData value, int numReduceTasks)
	{
		String mapKey=key.getHost();
	       System.out.println(numReduceTasks);
			if (mappingList.size() > 0 && mappingList.containsKey(mapKey.toLowerCase()))
			{
	           return mappingList.get(mapKey.toLowerCase());
			}
			else
			{
				try
				{
					newKey.set(mapKey);
					return hashPartitioner.getPartition(newKey, value, numReduceTasks);
				}
				catch (Exception e)
				{
					e.printStackTrace();
					return (int) (Math.random() * numReduceTasks);
				}
			}
	}

	@Override
	public void configure(JobConf conf)
	{	
		String mappingStr = conf.get("map.input.nodes");
		if (mappingStr != null)
		{
			String[] pairs = mappingStr.split(";");
			for (int i = 0; i < pairs.length; i++)
			{
				String[] pair = pairs[i].split(":");
				mappingList.put(pair[0].toLowerCase(), Integer.parseInt(pair[1]));
			}
		}


	}
}
