package com.accumulo.playground;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.acuumulo.playground.ImmutableMapBuilder;
import com.google.common.collect.Maps;

public class ImmutableMapBuilderTest {
	
	@Test
	public void testBuilder(){
		
		Map<String,String> map = ImmutableMapBuilder.<String,String>builder().put("key", "value").build();
		int match_size = 1;
		assertEquals(String.format("Map size should be %d after init (new).",match_size),match_size,map.keySet().size());
		assertEquals("Map key should be 'value'",map.get("key"),"value");
		
		Map<String,String> cmap = map;
		map = ImmutableMapBuilder.<String,String>builder().putAll(cmap).build();
		match_size = 1;
		assertEquals(String.format("Map size should be %d after init (with #setMap).",match_size),match_size,map.keySet().size());
		assertEquals("Map key should be 'value'",map.get("key"),"value");
		
		//MUST DO MUTATIONS WITHIN HASH MAP
		cmap = Maps.newHashMap(map);
		cmap.put("key", "");
		map = ImmutableMapBuilder.<String,String>builder().putAll(cmap).build();
		match_size = 1;
		assertEquals(String.format("Map size should be %d after init (with #setMap + overwrite).",match_size),match_size,map.keySet().size());
		assertEquals("Map key should be empty",map.get("key"),"");
	}
}
