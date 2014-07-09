package block.distributor;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class RowData implements WritableComparable<Object>
{
	private List<String> columns;
	private String fileName;

	public RowData()
	{
		super();
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		int size = in.readInt();
		columns = new ArrayList<String>();
		for (int i = 0; i < size; i++)
		{
			columns.add(in.readUTF());
		}
		fileName = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		int size = columns.size();
		out.writeInt(size);
		for (int i = 0; i < columns.size(); i++)
		{
			out.writeUTF(columns.get(i));
		}
		out.writeUTF(fileName);
	}

	public void setColumns(List<String> columns)
	{
		this.columns = columns;
	}

	public List<String> getColumns()
	{
		return columns;
	}

	@Override
	public int compareTo(Object o)
	{
		// TODO Auto-generated method stub
	return 0;
	}

	void setFileName(String fileName)
	{
		this.fileName = fileName;
	}

	String getFileName()
	{
		return fileName;
	}

}
