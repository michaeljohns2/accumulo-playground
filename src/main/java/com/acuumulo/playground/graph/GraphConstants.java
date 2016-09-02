package com.acuumulo.playground.graph;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.util.Pair;

public interface GraphConstants {
	
	public static enum Memento {
		NODE, EDGE;
	}

	public static enum MementoPart {
		ID, TYPE, ATTR; //WEIGHT?
	}
	
	public static enum RowIdPart {
		NODE_PREFIX_PART, NODE_ID_PART, NODE_ROWID, EDGE_ROWID;
		
		/**
		 * Strings for matching parts.
		 *  
		 * @param rowIdParts
		 * @param matchRowIdPart
		 * @return List<String>
		 */
		public static List<String> partsFrom(List<Pair<String,RowIdPart>> rowIdParts, RowIdPart matchRowIdPart){
			
			List<String> list = new ArrayList<>();
			
			for (Pair<String,RowIdPart> pair : rowIdParts){
				if (pair.getSecond().equals(matchRowIdPart))
					list.add(pair.getFirst());
			}
			
			return list;
		}
	}

	
	final public static String NODE_ROWID_SEPARATOR = ":";
	final public static String EDGE_ROWID_SEPARATOR = "|";
	
	final public static String NODE_CF = "node";
	final public static String EDGE_CF = "edge";
	
	final public static String TYPE_CQ = "type";
	final public static String NODE_PREFIX_CQ = "prefix";
	final public static String NODE_ID_CQ = "id";
	final public static String EDGE_ID1_CQ = "node_id_1";
	final public static String EDGE_ID2_CQ = "node_id_2";
	
	
}
