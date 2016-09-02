package com.accumulo.playground.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.junit.Test;

import com.accumulo.playground.BasePlayTest;
import com.acuumulo.playground.AccScan;
import com.acuumulo.playground.AccTable;
import com.acuumulo.playground.AccUtils;
import com.acuumulo.playground.graph.AccGraphEdgeWriter;
import com.acuumulo.playground.graph.EdgeRowId;

public class AccGraphEdgeWriterTest extends BasePlayTest {
	
	@Test	
	public void testInsertAndScan() {
		
//		DEBUG_LOG = true;//turn on for this test?
		
		BatchWriter writer = null;
		
		try{
			
			String prefixA = "m1";
			String idA = "e2";
			String prefixB = "m1";
			String idB = "e1";
			
			String edgeType = "to";
			
			Properties attrs = new Properties();
			
			writer = AccTable.createBatchWriter(aConn, table);
			
			AccGraphEdgeWriter.writeEdge(writer, prefixA, idA, prefixB, idB,edgeType, attrs, null);
			writer.close();//must close (will flush) before scanning
			
			results = AccScan.scanTable(aConn, table, auths, 0);
			int match_size = 3;
			String msg = String.format("Expected %d results for scan of table '%s' ",match_size,table);	
			if (DEBUG_LOG){
				System.out.println("\n"+msg);
				System.out.println(AccUtils.prettyStr(results, 0));
			}
			assertEquals(msg,match_size,results.size());
			
			/* scan for rowId */    
			String rowId = EdgeRowId.assembleEdgeRowId(prefixA, idA, prefixB, idB);
			
			assertEquals("Expect rowIds to be the same (A,B switch)",rowId,EdgeRowId.assembleEdgeRowId(prefixB, idB, prefixA, idA));
			
			results = AccScan.scanRowPublic(aConn,table,rowId);
			
			match_size = 3;
			msg = String.format("Expected %d results for scan of rowid '%s' ",match_size,rowId);	
			if (DEBUG_LOG){
				System.out.println("\n"+msg);
				System.out.println(AccUtils.prettyStr(results, 0));
			}
			
			assertEquals(msg,match_size,results.size());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
				try {
					writer.close();	// just in case there was a failure prior to in-line close				
				} catch (MutationsRejectedException e) {
					fail(e.getMessage());
				}
		}
	}
}
