package com.acuumulo.playground.graph;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.accumulo.core.util.Pair;

public class EdgeRowId extends MementoRowId{

	/**
	 * Assemble standardized Edge RowId.
	 * Will lexicographically sort (ascending) based on results of {@link #generateNodeId(String, String)} for A and B.
	 * 
	 * @param prefixA String prefix for node (optional)
	 * @param idA String node id (required)
	 * @param prefixB String prefix for node (optional)
	 * @param idB String node id (required)
	 * @return String
	 * 
	 * @throws InvalidParameterException
	 */
	public static String assembleEdgeRowId(String prefixA, String idA, String prefixB, String idB)
			throws InvalidParameterException{
		return assembleEdgeRowId(
				NodeRowId.assembleNodeRowId(prefixA, idA),
				NodeRowId.assembleNodeRowId(prefixB, idB),
				false, //don't skip standardization
				false //don't skip sort
				);
	}

	/**
	 * Order node for edge.
	 * Will lexicographically sort (ascending) A and B.
	 * 
	 * @param nodeRowIdA String Node A RowId
	 * @param nodeRowIdB String Node B RowId
	 * 
	 * @return String[]
	 */
	public static String[] orderNodesForEdge(String nodeRowIdA, String nodeRowIdB){
		String [] nodes = {nodeRowIdA,nodeRowIdB};		
		Arrays.sort(nodes);//ascending order in place.	
		return nodes;
	}

	/**
	 * Assemble standardized Edge RowId.
	 * Will lexicographically sort (ascending) A and B.
	 * 
	 * @param nodeRowIdA String Node A RowId
	 * @param nodeRowIdB String Node B RowId
	 * @param skipStandard boolean
	 * @param skipSort boolean
	 * @return String
	 * 
	 * @throws InvalidParameterException
	 */
	public static String assembleEdgeRowId(String nodeRowIdA, String nodeRowIdB, boolean skipStandard, boolean skipSort)
			throws InvalidParameterException{

		String nA = skipStandard ? nodeRowIdA : rowIdMustNotStandardizeToNull(nodeRowIdA);//may throw InvalidParameterException
		String nB = skipStandard ? nodeRowIdB : rowIdMustNotStandardizeToNull(nodeRowIdB);//may throw InvalidParameterException		
		String[] nodes = skipSort ? new String[] {nA,nB} : orderNodesForEdge(nA,nB);	

		return nodes[0]+EDGE_ROWID_SEPARATOR+nodes[1];		
	}

	/**
	 * Assemble standardized Edge RowId from list of parts.
	 * 
	 * @param rowIdParts List<Pair<String,RowIdPart>>	 
	 * @return String
	 * 
	 * @throws InvalidParameterException
	 */
	public static String assembleEdgeRowId(List<Pair<String,RowIdPart>> rowIdParts) 
			throws InvalidParameterException{

		String n1 = null;
		String n2 = null;

		for (Pair<String,RowIdPart> pair : rowIdParts){
			switch(pair.getSecond()){
			case EDGE_ROWID:
				return pair.getFirst();
			case NODE_ROWID:
				if (n1 == null) n1 = pair.getFirst();
				else if (n2 == null) n2 = pair.getFirst();
				break;			
			default:
				//nothing
			}
		}

		//skip checks
		return assembleEdgeRowId(n1,n2,true,true);		 
	}

	/**
	 * Generate standardized Edge RowId Parts.
	 * 
	 * @param nodeRowIdA String Node A RowId
	 * @param nodeRowIdB String Node B RowId
	 * @param skipStandard
	 * @param skipSort	 
	 * @return List<Pair<String,RowIdPart>> 
	 * 
	 * @throws InvalidParameterException
	 */
	public static List<Pair<String,RowIdPart>> generateEdgeRowIdParts(String nodeRowIdA, String nodeRowIdB, boolean skipStandard, boolean skipSort)
			throws InvalidParameterException{

		String nA = skipStandard ? nodeRowIdA : rowIdMustNotStandardizeToNull(nodeRowIdA);//may throw InvalidParameterException
		String nB = skipStandard ? nodeRowIdB : rowIdMustNotStandardizeToNull(nodeRowIdB);//may throw InvalidParameterException		
		String[] nodes = skipSort ? new String[] {nA,nB} : orderNodesForEdge(nA,nB);	

		List<Pair<String,RowIdPart>> list = new ArrayList<>();
		list.add(new Pair<String,RowIdPart>(assembleEdgeRowId(nodes[0], nodes[1], true, true),RowIdPart.EDGE_ROWID));
		list.add(new Pair<String,RowIdPart>(nodes[0],RowIdPart.NODE_ROWID));
		list.add(new Pair<String,RowIdPart>(nodes[1],RowIdPart.NODE_ROWID));

		return list;
	}

	/**
	 * Parse Node row id parts.
	 * @param rowId String
	 * @return List<Pair<String,RowIdPart>>
	 * 
	 * @throws InvalidParameterException
	 */
	public static List<Pair<String,RowIdPart>> parseEdgeRowIdParts(String rowId)
			throws InvalidParameterException{

		String rid = rowIdMustNotStandardizeToNull(rowId);//may throw InvalidParameterException

		List<Pair<String,RowIdPart>> list = new ArrayList<>();
		list.add(new Pair<String,RowIdPart>(rid,RowIdPart.EDGE_ROWID));

		if (!rid.contains(EDGE_ROWID_SEPARATOR)){
			return list;
		} 

		StringTokenizer st = new StringTokenizer(rid,EDGE_ROWID_SEPARATOR);
		list.add(new Pair<String,RowIdPart>(st.nextToken(),RowIdPart.NODE_ROWID)); //node id 1 is first
		list.add(new Pair<String,RowIdPart>(st.nextToken(),RowIdPart.NODE_ROWID)); //node id 2 is second		
		return list;
	}

	public EdgeRowId(String rowId){
		super(rowId);
	}

	public EdgeRowId(List<Pair<String, RowIdPart>> rowIdParts) {
		super(rowIdParts);
	}

	@Override
	protected List<Pair<String, RowIdPart>> parseRowIdParts(String rowId) {
		return parseEdgeRowIdParts(rowId);
	}

	@Override
	protected String assembleRowId(List<Pair<String, RowIdPart>> rowIdParts) {
		return assembleEdgeRowId(rowIdParts);
	}

}
