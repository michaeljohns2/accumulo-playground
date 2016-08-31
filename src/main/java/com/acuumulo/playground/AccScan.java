package com.acuumulo.playground;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

public class AccScan {
	
	final private static Logger logger = Logger.getLogger(AccScan.class);
	
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
		
		Authorizations scanAuths = new Authorizations();
		if (null != auths) {
			scanAuths = new Authorizations((String[]) auths.toArray());
		}
		
		Scanner scanner = conn.createScanner(table, scanAuths);
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

}
