package com.acuumulo.playground.graph;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.util.Pair;

import com.acuumulo.playground.AccConstants;
import com.acuumulo.playground.AccUtils;

public abstract class MementoRowId implements AccConstants, GraphConstants{

	/**
	 * Standardize a string, e.g. rowId or part.
	 * 
	 * Uses ManOp EMPTY_TO_NULL, TRIM, and TO_LOWERCASE
	 * 
	 * @param s
	 * @return String | null
	 */
	public static String standardize(String s){
		return AccUtils.manipulate(s, ManOp.EMPTY_TO_NULL,ManOp.TRIM,ManOp.TO_LOWERCASE);
	}

	/**
	 * Convenience for throwing exception on row id standardization.
	 * 
	 * @param rowId String
	 * @return String
	 * 
	 * @throws InvalidParameterException
	 */
	public static String rowIdMustNotStandardizeToNull(String rowId)
			throws InvalidParameterException {

		return mustNotStandardizeToNull(rowId,String.format("rowId '%s' cannot be empty.", rowId));
	}

	/**
	 * Convenience for throwing exception on standardization.
	 * @param s String 
	 * @param msg String
	 * @return String
	 * 
	 * @throws InvalidParameterException
	 */
	public static String mustNotStandardizeToNull(String s, String msg)
			throws InvalidParameterException {
		String c = standardize(s);

		if (c == null)
			throw new InvalidParameterException(msg);

		return c;
	}

	protected String raw_rowId;//as provided
	protected String rowId;//as standardized or derived
	protected List<Pair<String,RowIdPart>> rowIdParts;//as provided or derived

	/**
	 * Constructor from row id, will parse rowIdParts.
	 * @param rowId String
	 */
	public MementoRowId(String rowId){
		this.setRowId(rowId);
	}

	/**
	 * Constructor from row id parts, will assemble rowId.
	 * @param rowIdParts List<Pair<String,RowIdPart>>
	 */
	public MementoRowId(List<Pair<String,RowIdPart>> rowIdParts){
		this.setRowIdParts(rowIdParts);
	}

	/**
	 * Parse row id parts for rowId provided.
	 * @param String rowId
	 * @return List<Pair<String,RowIdPart>>
	 */
	protected abstract List<Pair<String,RowIdPart>> parseRowIdParts(String rowId);

	/**
	 * Assemble row id from provided parts.
	 * @param List<Pair<String,RowIdPart>>
	 * @return String rowId
	 */
	protected abstract String assembleRowId(List<Pair<String,RowIdPart>> rowIdParts);

	//###############################################################################
	// GETTERS & SETTERS
	//###############################################################################

	public String getRaw_rowId() {
		return raw_rowId;
	}

	public String getRowId() {
		return rowId;
	}

	public void setRowId(String rowId) {
		this.raw_rowId = rowId;
		this.rowId = standardize(rowId);		
		this.rowIdParts = parseRowIdParts(this.rowId);//don't call set here (infinite regress)!
	}

	public List<Pair<String, RowIdPart>> getRowIdParts() {
		if (rowIdParts == null){
			rowIdParts = new ArrayList<>();//don't return null list
		}
		return rowIdParts;
	}

	public void setRowIdParts(List<Pair<String, RowIdPart>> rowIdParts) {
		this.rowIdParts = rowIdParts;
		this.setRowId(assembleRowId(this.rowIdParts));
	}
}
