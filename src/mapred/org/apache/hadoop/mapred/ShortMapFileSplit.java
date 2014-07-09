package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.fs.Path;

public class ShortMapFileSplit extends  org.apache.hadoop.mapreduce.InputSplit 
implements InputSplit {
private Path file;
private long start;
private long length;
private String[] hosts;
private int rowGroupId;
private String rowGroupName;

public ShortMapFileSplit() {
	// TODO Auto-generated constructor stub
}

/** Constructs a split.
* @deprecated
* @param file the file name
* @param start the position of the first byte in the file to process
* @param length the number of bytes in the file to process
*/
@Deprecated
public ShortMapFileSplit(Path file, long start, long length, JobConf conf) {
this(file, start, length, (String[])null);
}

/** Constructs a split with host information
*
* @param file the file name
* @param start the position of the first byte in the file to process
* @param length the number of bytes in the file to process
* @param hosts the list of hosts containing the block, possibly null
*/
public ShortMapFileSplit(Path file, long start, long length, String[] hosts) {
this.file = file;
this.start = start;
this.length = length;
this.hosts = hosts;
}

public ShortMapFileSplit(int rowGroupId,String rowGroupName,Path file, long start, long length, String[] hosts)
{
	this(file,start,length,hosts);
	this.rowGroupId=rowGroupId;
	this.rowGroupName=rowGroupName;
}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		  out.writeInt(rowGroupId);
		  UTF8.writeString(out, file.toString());
		  UTF8.writeString(out, rowGroupName);
		 
		    out.writeLong(start);
		    out.writeLong(length);
			UTF8.writeString(out, hosts[0]);
			
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		rowGroupId=in.readInt();
		file = new Path(UTF8.readString(in));
		rowGroupName=UTF8.readString(in);

	    start = in.readLong();
	    length = in.readLong();
		hosts = new String[1];
		hosts[0] = UTF8.readString(in);
	}

	  /** The file containing this split's data. */
	  public Path getPath() { return file; }
	  
	  /** The position of the first byte in the file to process. */
	  public long getStart() { return start; }
	  
	  public String toString() { return file + ":" + start + "+" + length; }
	
	@Override
	public long getLength(){
		// TODO Auto-generated method stub
		return length;
	}

	@Override
	  public String[] getLocations() throws IOException {
	    if (this.hosts == null) {
	      return new String[]{};
	    } else {
	      return this.hosts;
	    }
	}
	    
	  public int getRowGroupId(){
		  return rowGroupId;
	  }
	  
	  public String getRowGroupName(){
		  return rowGroupName;
	  }
	}

