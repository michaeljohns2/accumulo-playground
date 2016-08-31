package com.acuumulo.playground;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;

/**
 * Accumulo Filter Object class to support various pre-search and post-search operations.
 * The key may not be accepted by Accumulo as some client post-filtering operations support missing values.
 * 
 * @author mjohns
 *
 */
public class AccFilterObj {
    
	Key key;	
	PartialKey pk;
	String row;
	String cf;
	String cq;
	String cv;
	long ts;
	boolean deleted;
	
	/**
	 * Public constructor.
	 * Will reset all values to initial.
	 */
	public AccFilterObj(){
		this.resetValues();
	}
	
	/**
	 * init supports builder pattern.
	 * @return AccFilterObj
	 */
	public static AccFilterObj init(){		
		AccFilterObj af = new AccFilterObj();
		return af; 
	}
	
	public AccFilterObj resetValues(){
		key = null;
		pk = null;
		row = "";
		cf = "";
		cq = "";
		cv = "";
		ts = Long.MAX_VALUE;
		deleted = false;
		return this;
	}
	
	/**
	 * Generate Key, different standards for Keys for post-filter / client operations. 
	 * Hint: call {@link #generatePartialKey()} to quasi-validate for Accumulo operations.	 
	 * @return Key
	 */
	public Key generateKey(){
		key = new Key(row,cf,cq,cv,ts);
		return key;
	}
	
	/**
	 * Generate PartialKey, requires valid entries.
	 * @return PartialKey | null
	 */
	public PartialKey generatePartialKey(){
		
		if (row.length() > 0  && cf.length() > 0 && cq.length() > 0 &&
				cv.length() > 0 && ts >=0 && ts < Long.MAX_VALUE && deleted) return PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL;
		
		if (row.length() > 0  && cf.length() > 0 && cq.length() > 0 &&
			cv.length() > 0 && ts >=0 && ts < Long.MAX_VALUE) return PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME;
		
		if (row.length() > 0  && cf.length() > 0 &&
			cq.length() > 0 && cv.length() > 0) return PartialKey.ROW_COLFAM_COLQUAL_COLVIS;
		
		if (row.length() > 0  && cf.length() > 0 &&
				cq.length() > 0) return PartialKey.ROW_COLFAM_COLQUAL;
		
		if (row.length() > 0  && cf.length() > 0) return PartialKey.ROW_COLFAM;
		
		if (row.length() > 0) return PartialKey.ROW;
		
		return null;
	}
		
	/**
	 * Copy object in support of testing and builder pattern.
	 * @return AccFilterObj
	 */
	public AccFilterObj copyObj() {
	    AccFilterObj afo = AccFilterObj.init()	    	
	    	.row(this.row)
	    	.cf(this.cf)
	    	.cq(this.cq)
	    	.cv(this.cv)
	    	.ts(this.ts)
	    	.deleted(this.deleted);
	    
	    afo.key = this.generateKey();
	    afo.pk = this.generatePartialKey();
	    
	    return afo;
	}
	
	//###########################################
	// BUILDER PATTERN FOR SETTER
	//###########################################
	/**
	 * Supports builder pattern.
	 * @param row String
	 * @return AccFilterObj
	 */	
	public AccFilterObj row(String row) {
		this.row = row;
		return this;
	}
	
	/**
	 * Supports builder pattern.
	 * @param cf String
	 * @return AccFilterObj
	 */
	public AccFilterObj cf(String cf) {
		this.cf = cf;
		return this;
	}
	
	/**
	 * Supports builder pattern.
	 * @param cq String
	 * @return AccFilterObj
	 */
	public AccFilterObj cq(String cq) {
		this.cq = cq;
		return this;
	}
	
	/**
	 * Supports builder pattern.
	 * @param cv String
	 * @return AccFilterObj
	 */
	public AccFilterObj cv(String cv) {
		this.cv = cv;
		return this;
	}
	
	/**
	 * Supports builder pattern.
	 * @param ts long
	 * @return AccFilterObj
	 */
	public AccFilterObj ts(long ts) {
		this.ts = ts;
		return this;
	}
	
	/**
	 * Supports builder pattern.
	 * @param deleted boolean
	 * @return AccFilterObj
	 */
	public AccFilterObj deleted(boolean deleted) {
		this.deleted = deleted;
		return this;
	}

	//###########################################
	// GETTER
	//###########################################	
	public String getRow() {
		return row;
	}	
	public String getCf() {
		return cf;
	}	
	public String getCq() {
		return cq;
	}	
	public String getCv() {
		return cv;
	}	
	public long getTs() {
		return ts;
	}
	public boolean getDeleted() {
		return deleted;
	}
}
