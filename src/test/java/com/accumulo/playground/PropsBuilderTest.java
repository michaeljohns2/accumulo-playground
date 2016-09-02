package com.accumulo.playground;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

import com.acuumulo.playground.PropsBuilder;

public class PropsBuilderTest {
	
	@Test
	public void testBuilder(){
		
		Properties props = PropsBuilder.init().add("key", "value").getProps();
		int match_size = 1;
		assertEquals(String.format("Properties size should be %d after init (new).",match_size),match_size,props.stringPropertyNames().size());
		assertEquals("Properties key should be 'value'",props.getProperty("key"),"value");
		
		Properties cprops = props;
		props = PropsBuilder.init().setProps(cprops).getProps();
		match_size = 1;
		assertEquals(String.format("Properties size should be %d after init (with #setProps).",match_size),match_size,props.stringPropertyNames().size());
		assertEquals("Properties key should be 'value'",props.getProperty("key"),"value");
		
		props = PropsBuilder.init().setProps(cprops).add("key", "").getProps();//note: null not allowed!
		match_size = 1;
		assertEquals(String.format("Properties size should be %d after init (with #setProps + overwrite).",match_size),match_size,props.stringPropertyNames().size());
		assertEquals("Properties key should be empty",props.getProperty("key"),"");
	}
}
