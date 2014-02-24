package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

public class xIndexUtils {

	public static int currentColumnNr = 0;
	private static String splitData = "";
	private static long blockIdOfFirstBlock = 0;

	// <attribute nr, <attribute value, blockId>>
	public final static TreeMap<Integer, TreeMap<String, TreeSet<Long>>> index = new TreeMap<Integer, TreeMap<String, TreeSet<Long>>> ();
	private static TreeMap<String, TreeSet<Long>> currentColumnIndex = null;
	
	//first block of split N -> first block of split N, second block of split N, third block of split N... 
	private static TreeMap<Long, List<Long>> block2split = new TreeMap<Long, List<Long>>();
	
	public static void addPacketToIndex(String data, long blockId, boolean lastPacket) {
		if(currentColumnIndex == null) {
			initializeIndexForCurrentColumn();
			
			if(currentColumnNr == 0) {
				blockIdOfFirstBlock = blockId;
				block2split.put(new Long(blockId), new ArrayList<Long>());
			}
		}

		String[] entriesToAdd;
		String tmp;

		/* the last entry on this packet is split between this and the following packet*/		 
		if((data.length() != data.lastIndexOf('\n') + 1) && !lastPacket) {
			entriesToAdd = data.substring(0, data.lastIndexOf('\n')).split("\n");
			tmp = data.substring(data.lastIndexOf('\n')+1);
		} else {
			entriesToAdd = data.split("\n");
			tmp = "";
		}

		if(splitData != "") {
			entriesToAdd[0] = splitData + entriesToAdd[0];
		}
		splitData = tmp;

		addEntriesToIndex(entriesToAdd, blockId);

		if(lastPacket) {
			ArrayList<Long> split = (ArrayList<Long>) block2split.get(blockIdOfFirstBlock);
			split.add(new Long(blockId));
			currentColumnIndex = null;
		}
	}

	private static void initializeIndexForCurrentColumn() {
		currentColumnIndex = index.get(new Integer(currentColumnNr));
		if(currentColumnIndex == null) {
			currentColumnIndex = new TreeMap<String, TreeSet<Long>>();
			index.put(new Integer(currentColumnNr), currentColumnIndex);
		}
	}

	private static void addEntriesToIndex(String[] entriesToAdd, long blockId) {
		for(String entry : entriesToAdd) {
			if(!currentColumnIndex.containsKey(entry)) {
				currentColumnIndex.put(entry, new TreeSet<Long>());
			}
			currentColumnIndex.get(entry).add(new Long(blockId));
		}
	}

	public static boolean checkIfRelevantSplit(TreeMap<Integer, String> filters, long blockId) {
		if(filters.size() == 0)
			return true;
		
		ArrayList<Long> split = (ArrayList<Long>) block2split.get(new Long(blockId));
		
		for(Integer attrNr : filters.keySet()) {
			String predicate = filters.get(attrNr);
			long blockIdOfAttrNr = split.get(attrNr.intValue()).longValue();
			
			TreeSet<Long> relevantBlocks = index.get(attrNr).get(predicate);
			
			if((relevantBlocks == null) || (!relevantBlocks.contains(new Long(blockIdOfAttrNr)))) {
				return false;
			}
		}

		return true;
	}
}