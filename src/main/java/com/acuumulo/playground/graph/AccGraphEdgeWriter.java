package com.acuumulo.playground.graph;

import java.security.InvalidParameterException;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;

import com.acuumulo.playground.AccConstants;
import com.acuumulo.playground.AccTable;
import com.acuumulo.playground.AccUtils;

public class AccGraphEdgeWriter implements AccConstants, GraphConstants{

	/**
	 * Store an edge in the graph according to custom conventions.
	 * 
	 * @param writer BatchWriter
	 * @param eri EdgeRowId
	 * @param skipStandard boolean
	 * @param skipSort boolean
	 * @param edgeType String
	 * @param attrs Properties 
	 * @param auths String	
	 * 
	 * @throws MutationsRejectedException
	 * @throws InvalidParameterException
	 */
	public static void writeEdge(
			BatchWriter writer,
			EdgeRowId eri,
			boolean skipStandard,
			boolean skipSort,
			String edgeType,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException  {

		//from parts get nodes in array of min size 2
		String[] nodes = AccUtils.toMinArray(
				RowIdPart.partsFrom(eri.getRowIdParts(), RowIdPart.NODE_ROWID), //extract just node row ids				
				2, //should be 2 nodes
				null // if missing
				);

		//call to most efficient method, direct to skip checks.
		writeEdge(writer,eri.getRowId(),nodes[0],nodes[1],skipStandard,skipSort,edgeType,attrs,auths);
	}

	/**
	 * Store an edge in the graph according to custom conventions.
	 * 
	 * @param writer BatchWriter
	 * @param prefixA String
	 * @param idA String
	 * @param prefixB String 
	 * @param idB String
	 * @param edgeType String
	 * @param attrs Properties 
	 * @param auths String	
	 * 
	 * @throws MutationsRejectedException
	 * @throws InvalidParameterException
	 */
	public static void writeEdge(
			BatchWriter writer,
			String prefixA,
			String idA,
			String prefixB,
			String idB,
			String edgeType,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException  {

		String nA = NodeRowId.assembleNodeRowId(prefixA, idA);//may throw InvalidParameterException
		String nB = NodeRowId.assembleNodeRowId(prefixB, idB);//may throw InvalidParameterException

		//call to method which will assemble edgeRowId, direct to skip standard checks.
		writeEdge(writer,nA,nB,true,edgeType,attrs,auths);
	}

	/**
	 * Store an edge in the graph according to custom conventions.
	 * This is the most expedited write method.
	 *
	 * @param writer BatchWriter
	 * @param nodeRowIdA String
	 * @param nodeRowIdB String
	 * @param edgeType String
	 * @param attrs Properties 
	 * @param auths String
	 * 
	 * @throws InvalidParameterException	   
	 * @throws MutationsRejectedException 
	 */
	public static void writeEdge(
			BatchWriter writer,
			String nodeRowIdA,
			String nodeRowIdB,
			boolean skipStandard,
			String edgeType,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException {

		String nA = skipStandard? nodeRowIdA : MementoRowId.rowIdMustNotStandardizeToNull(nodeRowIdA);//may throw InvalidParameterException
		String nB = skipStandard? nodeRowIdB : MementoRowId.rowIdMustNotStandardizeToNull(nodeRowIdB);//may throw InvalidParameterException
		String[] nodes = EdgeRowId.orderNodesForEdge(nA, nB);

		String rid = EdgeRowId.assembleEdgeRowId(nodes[0], nodes[1], true, true);

		//call to most efficient method, direct to skip checks.
		writeEdge(writer,rid,nodes[0],nodes[1],true,true,edgeType,attrs,auths);
	}

	/**
	 * Store an edge in the graph according to custom conventions.
	 *
	 * @param writer BatchWriter
	 * @param edgeRowId String	 
	 * @param skipStandard boolean (this is for just edgeRowId)   
	 * @param edgeType String
	 * @param attrs Properties 
	 * @param auths String
	 * 
	 * @throws InvalidParameterException	   
	 * @throws MutationsRejectedException 
	 */
	public static void writeEdge(
			BatchWriter writer,
			String edgeRowId,
			boolean skipStandard,
			String edgeType,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException {

		String rid = skipStandard ? edgeRowId : MementoRowId.rowIdMustNotStandardizeToNull(edgeRowId);//may throw InvalidParameterException

		//from rowid get nodes in array of min size 2
		String[] nodes = AccUtils.toMinArray(
				RowIdPart.partsFrom(EdgeRowId.parseEdgeRowIdParts(rid), RowIdPart.NODE_ROWID), //extract just node row ids				
				2, //should be 2 nodes
				null // if missing
				);

		if (nodes[0] == null || nodes[1] == null) 
			throw new InvalidParameterException(String.format("edgeRowId '%s' must be parseable into 2 node row ids", edgeRowId));

		//call to most efficient method, direct to skip checks.
		writeEdge(writer,rid,nodes[0],nodes[1],true,true,edgeType,attrs,auths);
	}

	/**
	 * Store an edge in the graph according to custom conventions.
	 * This is the most expedited write method.
	 *
	 * @param writer BatchWriter
	 * @param edgeRowId String
	 * @param nodeRowIdA String
	 * @param nodeRowIdB String
	 * @param skipStandard boolean
	 * @param skipSort boolean
	 * @param edgeType String
	 * @param attrs Properties 
	 * @param auths String
	 * 
	 * @throws InvalidParameterException	   
	 * @throws MutationsRejectedException 
	 */
	public static void writeEdge(
			BatchWriter writer,
			String edgeRowId,
			String nodeRowIdA,
			String nodeRowIdB,
			boolean skipStandard,
			boolean skipSort,
			String edgeType,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException {

		String rid = skipStandard ? edgeRowId : MementoRowId.rowIdMustNotStandardizeToNull(edgeRowId);//may throw InvalidParameterException
		String nA = skipStandard ? nodeRowIdA : MementoRowId.rowIdMustNotStandardizeToNull(nodeRowIdA);//may throw InvalidParameterException
		String nB = skipStandard ? nodeRowIdB : MementoRowId.rowIdMustNotStandardizeToNull(nodeRowIdB);//may throw InvalidParameterException		
		String[] nodes = skipSort ? new String[] {nA,nB} : EdgeRowId.orderNodesForEdge(nA,nB);	

		AccTable.insertRow(writer, rid, EDGE_CF, TYPE_CQ, edgeType, auths);
		AccTable.insertRow(writer, rid, EDGE_CF, EDGE_ID1_CQ, nodes[0], auths);
		AccTable.insertRow(writer, rid, EDGE_CF, EDGE_ID2_CQ, nodes[1], auths);
		
		if (attrs != null){
			for (String key : attrs.stringPropertyNames()){

				String k = AccUtils.manipulate(key, ManOp.EMPTY_TO_NULL);
				if (k == null) continue;//skip empty key

				String v = AccUtils.manipulate(attrs.getProperty(key), ManOp.EMPTY_TO_NULL);
				if (v == null) continue;//skip empty value (need to use delete writer)

				AccTable.insertRow(writer, rid, EDGE_CF, k, v, auths);
			}
		}		
	}
}
