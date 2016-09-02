package com.accumulo.playground;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.acuumulo.playground.HashMapBuilder;

public class HashMapBuilderTest {
	
	@Test
	public void testBuilder(){
		
		Map<String,String> map = HashMapBuilder.<String,String>init().put("key", "value").getMap();
		int match_size = 1;
		assertEquals(String.format("Map size should be %d after init (new).",match_size),match_size,map.keySet().size());
		assertEquals("Map key should be 'value'",map.get("key"),"value");
		
		Map<String,String> cmap = map;
		map = HashMapBuilder.<String,String>init().setMap(cmap).getMap();
		match_size = 1;
		assertEquals(String.format("Map size should be %d after init (with #setMap).",match_size),match_size,map.keySet().size());
		assertEquals("Map key should be 'value'",map.get("key"),"value");
		
		map = HashMapBuilder.<String,String>init().setMap(cmap).put("key","").getMap();
		match_size = 1;
		assertEquals(String.format("Map size should be %d after init (with #setMap + overwrite).",match_size),match_size,map.keySet().size());
		assertEquals("Map key should be empty",map.get("key"),"");
	}
}
