/**
 * 
 */
package org.apache.hadoop.util;

import java.util.ArrayList;

/**
 * @author chathuri
 *
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