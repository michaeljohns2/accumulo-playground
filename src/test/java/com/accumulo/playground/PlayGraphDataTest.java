package com.accumulo.playground;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.acuumulo.playground.AccScan;
import com.acuumulo.playground.AccTable;
import com.acuumulo.playground.AccUtils;

public class PlayGraphDataTest extends BasePlayTest{
	
	@Test	
	public void testInsertAndScan() {
		try{
		
			URL dUrl = PlayGraphDataTest.class.getResource("play-graph-data.csv");
			
			Path filePath = Paths.get(dUrl.toURI());
			AccTable.loadCsvPublic(aConn, table, filePath, true);
			
			/* scan for rowId */    
			String rowId = "m1:e1";
			results = AccScan.scanRowPublic(aConn,table,rowId);
			
			//FIXME: ENHANCE SCAN CAPABILITY in AccScan
			
			System.out.println(AccUtils.prettyStr(results, 0));//for debugging

			int match_size = 3;
			assertEquals(String.format("Expected %d results for scan of rowid '%s' ",match_size,rowId), match_size,  results.size());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
