package com.acuumulo.playground;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import com.acuumulo.playground.graph.MementoRowId;

public class AccScan implements AccConstants{
	
	final private static Logger logger = Logger.getLogger(AccScan.class);
	
	/**
	 * Create a Batch Scanner.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param auths Authorizations
	 * @param numQueryThreads int
	 * @return Scanner
	 * 
	 * @throws TableNotFoundException
	 */
	public static BatchScanner createBatchScanner(Connector conn, String table, Authorizations auths, int numQueryThreads)
		throws TableNotFoundException {
		return conn.createBatchScanner(table, auths, numQueryThreads);
	}
	
	/**
	 * Create a Batch Scanner.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param auths List<String>
	 * @param numQueryThreads int
	 * @return Scanner
	 * 
	 * @throws TableNotFoundException
	 */
	public static BatchScanner createBatchScanner(Connector conn, String table, List<String> auths, int numQueryThreads)
		throws TableNotFoundException {
		return createBatchScanner(conn,table,AccAuths.generateScanAuths(auths),numQueryThreads);
	}
	
	/**
	 * Create a Public (no auths) Batch Scanner.
	 * 
	 * @param conn Connector
	 * @param table String	 
	 * @param numQueryThreads int
	 * @return Scanner
	 * 
	 * @throws TableNotFoundException
	 */
	public static BatchScanner createPublicBatchScanner(Connector conn, String table, int numQueryThreads)
		throws TableNotFoundException {
		return createBatchScanner(conn,table,AccAuths.generateScanAuths(null),numQueryThreads);
	}
	
	/**
	 * Create a Scanner.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param auths Authorizations
	 * @return Scanner
	 * 
	 * @throws TableNotFoundException
	 */
	public static Scanner createScanner(Connector conn, String table, Authorizations auths) 
			throws TableNotFoundException{
		
		return conn.createScanner(table, auths);
	}
	
	/**
	 * Create a Scanner.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param auths List<String>
	 * @return Scanner
	 * 
	 * @throws TableNotFoundException
	 */
	public static Scanner createScanner(Connector conn, String table, List<String> auths) 
			throws TableNotFoundException{
		
		return createScanner(conn,table,AccAuths.generateScanAuths(auths));
	}
	
	/**
	 * Create a Public (no auths) Scanner.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @return Scanner
	 * 
	 * @throws TableNotFoundException
	 */
	public static Scanner createPublicScanner(Connector conn, String table) throws TableNotFoundException{		
		return createScanner(conn,table, AccAuths.generateScanAuths(null));
	}
	
	/**
	 * Scan table, optionally up to limit.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param auths List<String>
	 * @param limit int
	 * @return Map<Key,Value>
	 * 
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static Map<Key,Value> scanTable(Connector conn, String table, List<String> auths, int limit)
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		
		HashMap<Key, Value> result = new HashMap<Key, Value>();		
				
		Scanner scanner = createScanner(conn,table,auths);
		
		int count = 0;
		for (Entry<Key,Value> entry : scanner) {
			count ++;
			result.put(entry.getKey(), entry.getValue());
			if (limit > 0 && count >= limit){
				logger.info(String.format("...reached limit %d in table %s scan, returning.",limit,table));
				break;
			}
		}
		
		return result;
	} 
	
	/**
	 * Scan for row in table.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param rowid String
	 * @param auths List<String>
	 * @return Map<Key,Value>
	 * 
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static Map<Key,Value> scanRow(Connector conn, String table, String rowid, List<String> auths)
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		
		HashMap<Key, Value> result = new HashMap<Key, Value>();
		Scanner scanner = createScanner(conn,table,auths);
		
		scanner.setRange(new Range(rowid));
		for (Entry<Key,Value> entry : scanner) {
			result.put(entry.getKey(), entry.getValue());
		}
		
		return result;
	}

	/**
	 * Scan for row in table with public / null auths.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param rowid String
	 * @return Map<Key,Value> 
	 * 
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static Map<Key,Value> scanRowPublic(Connector conn, String table, String rowid)
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		return scanRow(conn, table, rowid, null);
	}
	
//FIXME: FINISH!
	
//	/**
//	 * Return entries matching criteria.
//	 *
//	 * @param token String
//	 * @param kvPart KeyValuePart to test
//	 * @param scanQualifier ScanQualifier for matching
//	 * @param scanner Scanner
//	 * @param limit int, truncate results if > 0
//	 * @return Map<Key,Value>
//	 */
//	public static Map<Key,Value> scanForMatches(
//			String token,
//			KeyValuePart kvPart,
//			ScanQualifier scanQualifier,
//			Scanner scanner,
//			int limit
//			) {
//		
//		Map<Key,Value> results = new HashMap<>();
//		
//		String s = MementoRowId.standardize(token);
//		if (s == null &&
//			!ScanQualifier.EMPTY_OR_NULL.equals(scanQualifier) &&
//			!ScanQualifier.NOT_EMPTY_OR_NULL.equals(scanQualifier)) return results;
//		
//		boolean rangeSet = false;
//		if (KeyValuePart.KEY.equals(kvPart) || KeyValuePart.ROWID.equals(kvPart)){
//			switch(scanQualifier){		
//			case EXACT:
//				scanner.setRange(Range.exact(s));
//				rangeSet = true;
//				break;
//			case EXACT_SENSITIVE:
//				scanner.setRange(Range.exact(token));
//				rangeSet = true;
//				break;
//			case STARTS_WITH:
//				scanner.setRange(Range.prefix(s));
//				rangeSet = true;
//				break;
//			case STARTS_WITH_SENSITIVE:
//				scanner.setRange(Range.prefix(token));
//				rangeSet = true;
//				break;
//			default:
//				//scan full table
//			}
//		}
//		
//		Iterator<Map.Entry<Key, Value>> iter = scanner.iterator();
//		int count = 0;
//		while(iter.hasNext()){
//			count++;			
//			Entry<Key,Value> entry = iter.next();
//			
//			// test token if rangeSet is false
//			if (!rangeSet){
//				
//				String test = null;
//				
//			}
//			
//			// add results
//			results.put(entry.getKey(), entry.getValue());
//			if (limit > 0 && count >= limit)
//				return results;
//		}
//		
//		return results;
//	}
}
