package org.apache.hadoop.util;

import java.util.TreeMap;
import java.util.TreeSet;

public class xIndexUtils {

	public static int currentColumn = 0;
	private static String splitData = "";

	// <attribute nr, <attribute value, blockId>>
	public final static TreeMap<Integer, TreeMap<String, TreeSet<Long>>> index = new TreeMap<Integer, TreeMap<String, TreeSet<Long>>> ();
	private static TreeMap<String, TreeSet<Long>> currentColumnIndex = null;
	
	//map blockId -> attrNr
	private static TreeMap<Long, Integer> block2attrNr = new TreeMap<Long, Integer>();
	
	public static void addPacketToIndex(String data, long blockId, boolean lastPacket) {
		if(currentColumnIndex == null) {
			initializeIndexForCurrentColumn();
			block2attrNr.put(new Long(blockId), new Integer(currentColumn));
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
			currentColumnIndex = null;
		}
	}

	private static void initializeIndexForCurrentColumn() {
		currentColumnIndex = index.get(new Integer(currentColumn));
		if(currentColumnIndex == null) {
			currentColumnIndex = new TreeMap<String, TreeSet<Long>>();
			index.put(new Integer(currentColumn), currentColumnIndex);
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

	public static boolean checkIfRelevantBlock(TreeMap<Integer, String> filters, long blockId) {
		if(filters.size() == 0)
			return true;		
		
		Integer attrNr = block2attrNr.get(new Long(blockId));	
		System.out.println("block " + blockId + " refers to attribute number: " + attrNr);
		
		System.out.println(index);
		
		String attrValue = filters.get(attrNr);
		if(attrValue == null)//no filters for this attribute
			return true;
		

		System.out.println(attrValue);
		
		//TODO: vale a pena guardar estes relevantBlocks para n ter q os ir buscar a cada avaliacao de um novo bloco?
		
		TreeSet<Long> relevantBlocks = index.get(attrNr).get(attrValue);
		if(relevantBlocks == null)//no relevant blocks for this filter
			return false;

		System.out.println(relevantBlocks);

		return relevantBlocks.contains(new Long(blockId));
	}
}