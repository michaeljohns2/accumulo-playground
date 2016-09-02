package com.accumulo.playground.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.accumulo.playground.BasePlayTest;
import com.acuumulo.playground.AccScan;
import com.acuumulo.playground.AccTable;
import com.acuumulo.playground.AccUtils;

public class PlayGraphDataTest extends BasePlayTest{
	
	@Test	
	public void testInsertAndScan() {
		
//		DEBUG_LOG = true;//turn on for this test?
		
		try{
			URL dUrl = PlayGraphDataTest.class.getResource("play-graph-data.csv");
			
			Path filePath = Paths.get(dUrl.toURI());
			AccTable.loadCsv(aConn, table, filePath, null, true);
			
			/* scan for rowId */    
			String rowId = "m1:e1";
			results = AccScan.scanRowPublic(aConn,table,rowId);
			
			int match_size = 3;
			String msg = String.format("Expected %d results for scan of rowid '%s' ",match_size,rowId);	
			if (DEBUG_LOG){
				System.out.println("\n"+msg);
				System.out.println(AccUtils.prettyStr(results, 0));
			}
			assertEquals(msg,match_size,results.size());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
