package com.acuumulo.playground;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

/**
 * THIS IS NOW SUPERCEDED BY PlayTest
 * @author mjohns
 *
 */
public class PlayDriver {

	final private static Logger logger = Logger.getLogger(PlayDriver.class);

	//use mini cluster for development and initial testing
	static boolean useMini = true;
	static String table = "table1";
	static List<String> auths = Arrays.asList("A","B");

	/**
	 * Driver main method.
	 * 
	 * @param args String[]
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TableExistsException 
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws TableNotFoundException 
	 */
	public static void main(String[] args) 
			throws IOException, InterruptedException, AccumuloException, 
			AccumuloSecurityException, TableExistsException, TableNotFoundException{

		try{

			/* init */ 
			AccObj aObj = AccInstanceMgr.getAccObj(useMini);

			// reference to connector
			Connector aConn = aObj.getConnector();
			
			// make sure the root user scan auths set
			aObj.ensureRootAuths(auths);
			
			// make sure table is created
			AccTable.ensureTableCreated(aConn,table);


			/* insert some data, with auths */

			//									--------------------------------------
			//                  				rowid	cf		cq		val		auths
			//									--------------------------------------
			AccTable.insertRow(aConn,	table,	"a", 	"b",	"c",	"d",	"A|B");
			AccTable.insertRow(aConn,	table,	"a", 	"b",	"f",	"g", 	"A&B");
			AccTable.insertRow(aConn,	table,	"a", 	"b",	"i",	"j", 	"B");
			AccTable.insertRow(aConn,	table,	"a", 	"e",	"f",	"g", 	"A&B");
			AccTable.insertRow(aConn,	table,	"a", 	"h",	"i",	"j", 	"B");
			
			/* scan for rowId */
			String rowId = "a";
			Map<Key, Value> results = AccScan.scanRow(aObj.getConnector(),table, rowId, auths);			
			logger.info(String.format("scan for rowId '%s' returned '%d' results.", rowId,results.size()));

			/* filter */
			Key compKey;
			Map<Key, Value> fresults;
			AccFilterObj afo = AccFilterObj.init();
			String descrip;
			StringBuilder sb;
			
			/* by key matches */
			List<AccFilterObj> afos = Arrays.asList(
					afo.row("a").copyObj(),afo.cf("b").copyObj(),afo.cq("c").copyObj(),
					afo.cv("A|B").copyObj(),afo.resetValues().cv("A|B").copyObj());
			List<String> descrips = Arrays.asList("by row","plus cf","plus cq","plus cv","cv only");
			logger.info("::: Filter by keys :::");
			
			for (int i=0; i< afos.size(); i++){
				afo = afos.get(i);
				descrip = descrips.get(i);
				compKey = afo.generateKey();
				sb = new StringBuilder();
				sb.append("\n\n").append(String.format("<<<filter (%s) on key '%s'>>>", descrip, compKey)).append("\n");				
				fresults = AccFilter.filterResultsByKeyMatch(results, compKey);
				sb.append(String.format("results (%d) -->\n%s", fresults.size(), AccUtils.prettyStr(fresults, 0))).append("\n\n");
				logger.info(sb.toString());
			}
			
			/* by partial key matches */
			afo = AccFilterObj.init().row("a").cf("b").cq("c").cv("A|B");			
			compKey = afo.generateKey();
			logger.info(String.format("::: Partial Filter with key '%s' :::", compKey));
			
			for (PartialKey partialKey : PartialKey.values()){				
				sb = new StringBuilder();
				sb.append("\n\n").append(String.format("<<<filter with partial key '%s'>>>", partialKey.name())).append("\n");				
				fresults = AccFilter.filterResultsByPartialKeyMatch(results, compKey, partialKey);
				sb.append(String.format("results (%d) -->\n%s", fresults.size(), AccUtils.prettyStr(fresults, 0))).append("\n\n");
				logger.info(sb.toString());
			}

		} finally {
			//shut down cleanly on the way out
			AccInstanceMgr.stopAccObj(useMini);
		}
	}	
}
