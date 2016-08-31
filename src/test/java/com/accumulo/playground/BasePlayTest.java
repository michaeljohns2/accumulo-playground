package com.accumulo.playground;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.acuumulo.playground.AccInstanceMgr;
import com.acuumulo.playground.AccObj;
import com.acuumulo.playground.AccTable;

public class BasePlayTest {
	
	protected static AccObj aObj;	
	protected static Connector aConn;
	protected static boolean useMini = true;
	protected static String table = "table1";
	protected static List<String> auths = Arrays.asList("A","B"); 

	protected static Map<Key, Value> results;

	@BeforeClass
	public static void setUp() 
			throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException {

		/* init */ 
		aObj = AccInstanceMgr.getAccObj(useMini);

		// reference to connector
		aConn = aObj.getConnector();

		// make sure the root user scan auths set
		aObj.ensureRootAuths(auths);

		// make sure table is created
		AccTable.ensureTableCreated(aConn,table);

	}

	@AfterClass
	public static void tearDown() 
			throws IOException, InterruptedException {
		//shut down cleanly on the way out
		AccInstanceMgr.stopAccObj(useMini);
		
		//nullify generated variables
		aObj = null;
		aConn = null;
		results = null;
	}
}
