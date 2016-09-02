package com.accumulo.playground;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.acuumulo.playground.KVData;

import org.junit.Test;

import com.acuumulo.playground.AccConstants.KVPart;
import com.acuumulo.playground.AccConstants.ManOp;

public class KVDataTest {

	@Test
	public void testKVData(){

		try{
			KVData kvd = null;
			String smatch = null;

			smatch = "rowid";
			kvd = new KVData(KVPart.ROW_ID,smatch);
			assertEquals(String.format("Expect value to be '%s'",smatch),smatch,kvd.getExpectedDataAsString());

			smatch = "1";
			kvd = new KVData(KVPart.TIME_STAMP,Long.parseLong(smatch));
			assertEquals(String.format("Expect value to be '%s'",smatch),smatch,kvd.getExpectedDataAsString());

			smatch = "false";
			kvd = new KVData(KVPart.DEL,Boolean.parseBoolean(smatch));
			assertEquals(String.format("Expect value to be '%s'",smatch),smatch,kvd.getExpectedDataAsString());

			smatch = "test";
			kvd = new KVData(KVPart.VALUE, smatch.getBytes());
			assertEquals(String.format("Expect value to be '%s'",smatch),smatch,kvd.getExpectedDataAsString());

			smatch = "";
			kvd = new KVData(KVPart.VALUE, smatch.getBytes());
			assertEquals(String.format("Expect value to be '%s'",smatch),smatch,kvd.getExpectedDataAsString());

			smatch = null;
			kvd = new KVData(KVPart.VALUE, "".getBytes());
			assertEquals(String.format("Expect value to be '%s'",smatch),smatch,kvd.getExpectedDataAsString(ManOp.EMPTY_TO_NULL));
			
		} catch (Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
