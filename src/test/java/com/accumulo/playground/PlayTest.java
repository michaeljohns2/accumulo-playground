package com.accumulo.playground;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import com.acuumulo.playground.AccFilter;
import com.acuumulo.playground.AccFilterObj;
import com.acuumulo.playground.AccScan;
import com.acuumulo.playground.AccTable;
import com.acuumulo.playground.AccUtils;


public class PlayTest extends BasePlayTest{

	@Test
	public void testInsertAndScan() {
		try{
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

	@Test
	public void testPostFilterByKey(){
		try{
			//Make sure results have been populated
			if (results == null)
				testInsertAndScan();

			/* filter */
			Key compKey;
			AccFilterObj afo = AccFilterObj.init();
			Map<Key, Value> fresults;
			String descrip;


			/* by key matches */
			List<AccFilterObj> afos = Arrays.asList(
					afo.row("a").copyObj(),afo.cf("b").copyObj(),afo.cq("c").copyObj(),
					afo.cv("A|B").copyObj(),afo.resetValues().cv("A|B").copyObj());
			List<String> descrips = Arrays.asList("by row","plus cf","plus cq","plus cv","cv only");

			int match_size = 0;
			for (int i=0; i< afos.size(); i++){
				afo = afos.get(i);
				descrip = descrips.get(i);
				compKey = afo.generateKey();
				fresults = AccFilter.filterResultsByKeyMatch(results, compKey);

				switch(i){
				case 0:
					match_size = 5;					
					break;
				case 1:
					match_size = 3;					
					break;
				case 2:
					match_size = 1;					
					break;
				case 3:
					match_size = 1;					
					break;
				case 4:
					match_size = 1;
					break;    	
				default:
					match_size = 0;
				}
				
				String msg = String.format("Expected %d results for key %s (%s)",match_size,compKey,descrip);	
				if (DEBUG_LOG){
					System.out.println("\n"+msg);
					System.out.println(AccUtils.prettyStr(fresults, 0));
				}				
				assertEquals(msg, match_size,fresults.size());
			}    		

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPostFilterByPartialKey(){
		try{
			//Make sure results have been populated
			if (results == null)
				testInsertAndScan();

			/* filter */
			AccFilterObj afo = AccFilterObj.init().row("a").cf("b").cq("c").cv("A|B");			
			Key compKey = afo.generateKey();
			Map<Key, Value> fresults;

			int match_size = 0;
			for (PartialKey partialKey : PartialKey.values()){				
				fresults = AccFilter.filterResultsByPartialKeyMatch(results, compKey, partialKey);

				switch (partialKey){
				case ROW:
					match_size = 5;
					break;
				case ROW_COLFAM:
					match_size = 3;					
					break;	
				case ROW_COLFAM_COLQUAL:
				case ROW_COLFAM_COLQUAL_COLVIS:
					match_size = 1;					
					break;
				default:
					match_size = 0;					
				}
				
				String msg = String.format("Expected %d results for partialKey %s",match_size,partialKey.name());		
				if (DEBUG_LOG){
					System.out.println("\n"+msg);
					System.out.println(AccUtils.prettyStr(fresults, 0));
				}	
				assertEquals(msg,match_size,fresults.size());
				
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

