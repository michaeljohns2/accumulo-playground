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

/**
 * Extends PlayTest and load same data from file.
 * @author mjohns
 *
 */
public class PlayDataTest extends PlayTest{

	@Test
	@Override
	public void testInsertAndScan() {
		try{
			
			/* !!! Note: The data should be the same the data from PlayTest !!! */
			
			//									--------------------------------------
			//                  				rowid	cf		cq		val		auths
			//									--------------------------------------
//			AccTable.insertRow(aConn,	table,	"a", 	"b",	"c",	"d",	"A|B");
//			AccTable.insertRow(aConn,	table,	"a", 	"b",	"f",	"g", 	"A&B");
//			AccTable.insertRow(aConn,	table,	"a", 	"b",	"i",	"j", 	"B");
//			AccTable.insertRow(aConn,	table,	"a", 	"e",	"f",	"g", 	"A&B");
//			AccTable.insertRow(aConn,	table,	"a", 	"h",	"i",	"j", 	"B");

			URL dUrl = PlayDataTest.class.getResource("play-data.csv");
			
			Path filePath = Paths.get(dUrl.toURI());
			AccTable.loadCsv(aConn,table,filePath,null,true);
			
			/* scan for rowId */    
			String rowId = "a";
			results = AccScan.scanRow(aConn,table,rowId,auths);			

			int match_size = 5;
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

