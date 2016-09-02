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
import com.acuumulo.playground.PropsBuilder;
import com.acuumulo.playground.graph.AccGraphNodeWriter;
import com.acuumulo.playground.graph.NodeRowId;

public class AccGraphNodeWriterTest extends BasePlayTest {
	
	@Test	
	public void testInsertAndScan() {
		
//		DEBUG_LOG = true;//turn on for this test?
		
		BatchWriter writer = null;
		
		try{
			
			String prefix = "m1";
			String id = "e1";
			String type = "event";
			
			Properties attrs = PropsBuilder.init()
					.add("key", "value")
					.getProps();
			
			writer = AccTable.createBatchWriter(aConn, table);
			
			AccGraphNodeWriter.writeNode(writer, prefix, id, type, attrs, null);
			writer.close();//must close (will flush) before scanning
			
			results = AccScan.scanTable(aConn, table, auths, 0);
			int match_size = 4;
			String msg = String.format("Expected %d results for scan of table '%s' ",match_size,table);	
			if (DEBUG_LOG){
				System.out.println("\n"+msg);
				System.out.println(AccUtils.prettyStr(results, 0));
			}
			assertEquals(msg,match_size,results.size());
			
			/* scan for rowId */    
			String rowId = NodeRowId.assembleNodeRowId(prefix, id);			
			results = AccScan.scanRowPublic(aConn,table,rowId);
			
			match_size = 4;
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

