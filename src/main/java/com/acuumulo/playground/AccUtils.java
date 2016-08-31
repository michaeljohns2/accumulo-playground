package com.acuumulo.playground;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class AccUtils {
	
	/**
	 * Pretty String for Map. 
	 * @param map Map<Key,Value>
	 * @param limit int stop at size, < 1 means no limit.
	 * @return
	 */
	public static String prettyStr(Map<Key,Value> map, int limit){
		StringBuilder sb = new StringBuilder();
		
		int c = 0;
		for (Entry<Key,Value> entry : map.entrySet()){
			c++;
			Key key = entry.getKey();
			Value value = entry.getValue();
			
			sb.append("\t").append("'").append(key.toString()).append("' --> '").append(value.toString()).append("'\n");
			if (limit > 0 && c >= limit) break;
		}
		return sb.toString();
	}
}
