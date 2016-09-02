package com.acuumulo.playground.graph;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.util.Pair;

import com.acuumulo.playground.AccConstants;
import com.acuumulo.playground.AccTable;
import com.acuumulo.playground.AccUtils;

public class AccGraphNodeWriter implements AccConstants, GraphConstants{

	/**
	 * Store a node in the graph according to custom conventions.
	 * 
	 * @param writer BatchWriter
	 * @param nri NodeRowId
	 * @param skipStandard boolean	 
	 * @param type String
	 * @param attrs Properties 
	 * @param auths String	
	 * 
	 * @throws MutationsRejectedException
	 * @throws InvalidParameterException
	 */
	public static void writeNode(
			BatchWriter writer,
			NodeRowId nri,
			boolean skipStandard,			
			String type,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException  {
		
		String n = skipStandard? nri.getRowId() : MementoRowId.rowIdMustNotStandardizeToNull(nri.getRowId());//may throw InvalidParameterException
        
		//need parts of node row id
		List<Pair<String,RowIdPart>> parts = nri.getRowIdParts();
		String prefix = AccUtils.toMinArray(RowIdPart.partsFrom(parts,RowIdPart.NODE_PREFIX_PART),1,null)[0];//may be null
		String id = AccUtils.toMinArray(RowIdPart.partsFrom(parts,RowIdPart.NODE_ID_PART),1,null)[0];//may be null
		
		id = skipStandard? id : MementoRowId.mustNotStandardizeToNull(id,"'id' cannot be empty ('prefix' is optional).");//may throw InvalidParameterException
		
		//call to method which will write, direct to skip standard checks.
		writeNode(writer,n,prefix,id,true,type,attrs,auths);
	}

	/**
	 * Store a node in the graph according to custom conventions.
	 * 
	 * @param writer BatchWriter
	 * @param prefix String
	 * @param id String	 
	 * @param type String
	 * @param attrs Properties 
	 * @param auths String	
	 * 
	 * @throws MutationsRejectedException
	 * @throws InvalidParameterException
	 */
	public static void writeNode(
			BatchWriter writer,
			String prefix,
			String id,			
			String type,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException  {

		String n = NodeRowId.assembleNodeRowId(prefix, id);//may throw InvalidParameterException
		String p = MementoRowId.standardize(prefix);//use standardized
		String i = MementoRowId.standardize(id);//use standardized

		//call to method which will write, direct to skip standard checks.
		writeNode(writer,n,p,i,true,type,attrs,auths);
	}

	/**
	 * Store a node in the graph according to custom conventions.	 * 
	 *
	 * @param writer BatchWriter
	 * @param nodeRowId String
	 * @param skipStandard	 
	 * @param type String
	 * @param attrs Properties 
	 * @param auths String
	 * 
	 * @throws InvalidParameterException	   
	 * @throws MutationsRejectedException 
	 */
	public static void writeNode(
			BatchWriter writer,
			String nodeRowId,			
			boolean skipStandard,
			String type,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException {

		String n = skipStandard? nodeRowId : MementoRowId.rowIdMustNotStandardizeToNull(nodeRowId);//may throw InvalidParameterException
        
		//need parts of node row id
		List<Pair<String,RowIdPart>> parts = NodeRowId.parseNodeRowIdParts(n);
		String prefix = AccUtils.toMinArray(RowIdPart.partsFrom(parts,RowIdPart.NODE_PREFIX_PART),1,null)[0];//may be null
		String id = AccUtils.toMinArray(RowIdPart.partsFrom(parts,RowIdPart.NODE_ID_PART),1,null)[0];//may be null
		
		id = skipStandard? id : MementoRowId.mustNotStandardizeToNull(id,"'id' cannot be empty ('prefix' is optional).");
		
		//call to method which will write, direct to skip standard checks.
		writeNode(writer,n,prefix,id,true,type,attrs,auths);
	}
	
	/**
	 * Store a node in the graph according to custom conventions.
	 * This is the most expedited write method.
	 *
	 * @param writer BatchWriter
	 * @param nodeRowId String
	 * @param prefix String
	 * @param id String
	 * @param boolean skipStandard	 
	 * @param type String
	 * @param attrs Properties 
	 * @param auths String
	 * 
	 * @throws InvalidParameterException	   
	 * @throws MutationsRejectedException 
	 */
	public static void writeNode(
			BatchWriter writer,
			String nodeRowId,
			String prefix,
			String id,
			boolean skipStandard,
			String type,
			Properties attrs,
			String auths
			) throws InvalidParameterException, MutationsRejectedException {

		String n = skipStandard? nodeRowId : MementoRowId.rowIdMustNotStandardizeToNull(nodeRowId);//may throw InvalidParameterException
		String p = skipStandard? prefix : MementoRowId.standardize(prefix);//may be null
		String i = skipStandard? id : MementoRowId.mustNotStandardizeToNull(id,"'id' cannot be empty ('prefix' is optional).");//may throw InvalidParameterException

		AccTable.insertRow(writer, n, NODE_CF, TYPE_CQ, type, auths);//add type
		if (p != null) AccTable.insertRow(writer, n, NODE_CF, NODE_PREFIX_CQ, p, auths);//add prefix if not null
		AccTable.insertRow(writer, n, NODE_CF, NODE_ID_CQ, i, auths);//add id
		
		if (attrs != null){
			for (String key : attrs.stringPropertyNames()){

				String k = AccUtils.manipulate(key, ManOp.EMPTY_TO_NULL);
				if (k == null) continue;//skip empty key

				String v = AccUtils.manipulate(attrs.getProperty(key), ManOp.EMPTY_TO_NULL);
				if (v == null) continue;//skip empty value (need to use delete writer)

				AccTable.insertRow(writer, n, NODE_CF, k, v, auths);
			}
		}
	}
}
