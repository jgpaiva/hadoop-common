package block.distributor;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable
{
	private String host;
	private String groupValue;

	public CompositeKey()
	{

	}

	public CompositeKey(String host, String groupValue)
	{
		this.host = host;
		this.groupValue = groupValue;
	}

	@Override
	public String toString()
	{

		return (new StringBuilder()).append(host).append(',')
				.append(groupValue).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		host = WritableUtils.readString(in);
		groupValue = WritableUtils.readString(in);

	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, host);
		WritableUtils.writeString(out, groupValue);
	}

	@Override
	public int compareTo(Object o)
	{
		// TODO Auto-generated method stub
		CompositeKey c = (CompositeKey) o;
		int result = host.compareTo(c.host);
		if (result == 0)
		{
			result = groupValue.compareTo(c.groupValue);
		}
		return result;
	}

	public String getHost()
	{
		return host;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public String getGroupValue()
	{
		return groupValue;
	}

	public void setGroupValue(String groupVal)
	{
		this.groupValue = groupVal;
	}
}
