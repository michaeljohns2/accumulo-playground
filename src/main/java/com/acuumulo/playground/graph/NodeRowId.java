package com.acuumulo.playground.graph;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.accumulo.core.util.Pair;

public class NodeRowId extends MementoRowId{
	
	/**
	 * Assemble standardized Node RowId.
	 * 
	 * @param prefix String prefix for node (optional)
	 * @param id String node id (required)
	 * @return String
	 * 
	 * @throws InvalidParameterException
	 */
	public static String assembleNodeRowId(String prefix, String id) 
			throws InvalidParameterException{

		String p = standardize(prefix);
		String i = standardize(id);
		
		if (p != null && i != null) return p+NODE_ROWID_SEPARATOR+i;
		else if (i != null) return i;
		else throw new InvalidParameterException("'id' cannot be empty ('prefix' is optional)."); 
	}
	
	/**
	 * Generate standardized Node RowId Parts.
	 * 
	 * @param prefix String prefix for node (optional)
	 * @param id String node id (required)
	 * @return List<Pair<String,RowIdPart>>
	 * 
	 * @throws InvalidParameterException
	 */
	public static List<Pair<String,RowIdPart>> generateNodeRowIdParts(String prefix, String id){
		List<Pair<String,RowIdPart>> list = new ArrayList<>();
		
		String p = standardize(prefix);
		String i = standardize(id);
		
		if (p != null && i != null){
			list.add(new Pair<String,RowIdPart>(p+NODE_ROWID_SEPARATOR+i,RowIdPart.NODE_ROWID));
			list.add(new Pair<String,RowIdPart>(p,RowIdPart.NODE_PREFIX_PART));		
			return list;
		} else if (i != null){
			list.add(new Pair<String,RowIdPart>(i,RowIdPart.NODE_ROWID));
			list.add(new Pair<String,RowIdPart>(i,RowIdPart.NODE_ID_PART));
			return list;
		} else throw new InvalidParameterException("'id' cannot be empty ('prefix' is optional).");		
	}
	
	/**
	 * Assemble standardized Node RowId from list of parts.
	 * 
	 * @param rowIdParts List<Pair<String,RowIdPart>>	 
	 * @return String
	 * 
	 * @throws InvalidParameterException
	 */
	public static String assembleNodeRowId(List<Pair<String,RowIdPart>> rowIdParts) 
			throws InvalidParameterException{

		String prefix = null;
		String id = null;
		
		for (Pair<String,RowIdPart> pair : rowIdParts){
			switch(pair.getSecond()){
			case NODE_ROWID:
				return pair.getFirst();
			case NODE_PREFIX_PART:
				prefix = pair.getFirst();
				break;
			case NODE_ID_PART:
				id = pair.getFirst();
				break;
			default:
				//nothing
			}
		}
		return assembleNodeRowId(prefix,id);		 
	}

	/**
	 * Parse Node row id parts.
	 * @param rowId String
	 * @return List<Pair<String,RowIdPart>>
	 * 
	 * @throws InvalidParameterException
	 */
	public static List<Pair<String,RowIdPart>> parseNodeRowIdParts(String rowId)
		throws InvalidParameterException {
		
		String rid = rowIdMustNotStandardizeToNull(rowId);//may throw InvalidParameterException
		
		List<Pair<String,RowIdPart>> list = new ArrayList<>();
		list.add(new Pair<String,RowIdPart>(rid,RowIdPart.NODE_ROWID));
		
		if (!rid.contains(NODE_ROWID_SEPARATOR)){			 
			list.add(new Pair<String,RowIdPart>(rid,RowIdPart.NODE_ID_PART));
			return list;
		} 
		
		StringTokenizer st = new StringTokenizer(rid,NODE_ROWID_SEPARATOR);
		list.add(new Pair<String,RowIdPart>(st.nextToken(),RowIdPart.NODE_PREFIX_PART)); //prefix is first
		list.add(new Pair<String,RowIdPart>(st.nextToken(),RowIdPart.NODE_ID_PART)); //id is second		
		return list;
	}
	
	public NodeRowId(String rowId){
		super(rowId);
	}
	
	public NodeRowId(List<Pair<String, RowIdPart>> rowIdParts) {
		super(rowIdParts);
	}

	@Override
	protected List<Pair<String, RowIdPart>> parseRowIdParts(String rowId) {
		return parseNodeRowIdParts(rowId);
	}

	@Override
	protected String assembleRowId(List<Pair<String,RowIdPart>> rowIdParts) {
		return assembleNodeRowId(rowIdParts);
	}
}
