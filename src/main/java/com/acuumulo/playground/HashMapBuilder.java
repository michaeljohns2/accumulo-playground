package com.acuumulo.playground;

import java.util.HashMap;
import java.util.Map;

/**
 * A builder pattern for Map.
 * If mutability is not needed, use ImmutableMapBuilder.
 * 
 * Example `HashMapBuilder.<String,String>init().put("test", "test").getMap();`
 * 
 * @author mjohns
 *
 */
public class HashMapBuilder<K,V> {
	
	public static <K,V> HashMapBuilder<K, V> init(){
		return new HashMapBuilder<K,V>();
	}	
	
	private Map<K,V> map;
		
	public HashMapBuilder(){
		map = new HashMap<>();
	}
	
	public HashMapBuilder<K,V> put(K key, V value){
		map.put(key, value);
		return this;
	}
	
	public Map<K,V> getMap() {
		return map;
	}

	public HashMapBuilder<K,V> setMap(Map<K,V> map) {
		this.map = map;
		return this;
	}
}