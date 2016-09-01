package com.acuumulo.playground;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class AccUtils implements AccConstants{
	
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
	
	/**
	 * Intended for smaller lists, will populate an array to ensure a minimum count of values.
	 * Useful in tuple-like manipulations.
	 *  
	 * @param list List<String>
	 * @param minSize if > 0 then applied
	 * @param padVal String | null to pad if minSize applied
	 * @return String[]
	 */
	public static String[] toMinArray(final List<String> list, int minSize, String padVal){
		
		int min = Math.max(minSize,0);// min will be 0+
		int ls = list == null? 0 : list.size();//ls will be 0+
		int as = Math.max(min, ls);// get a size
		int diff = min - ls;//for value > 0 must pad
		
		String[] a = new String[as];//allocate size
		
		//populate list values
		if (list != null){
			for (int i=0; i<list.size(); i++)
				a[i] = list.get(i);
		}	
		
		//pad if needed
		if (diff > 0){
			for (int i=0; i<diff; i++)
				a[i] = padVal;
		}
		
		return a;
	}
	
	/**
	 * Apply ManOp(s) to string to ensure conformance of manipulation. 
	 * @param s String
	 * @param manops ManOps vararg 
	 * @return copy of s with ManOp(s) applied.
	 */
	public static String manipulate(String s, ManOp... manops){
		
		String c = s;
		
		if (manops == null) return c;
				
		for (ManOp m : manops){
			switch(m){
			case EMPTY_TO_NULL:
				if (c == null || c.trim().isEmpty()) c = null;
				break;
			case NULL_TO_EMPTY:
				if (c == null) c = "";
				break;
			case TO_LOWERCASE:
				if (c != null) c = c.toLowerCase();
			case TRIM:
				if (c != null) c = c.trim();
			}
		}
		
		return c;
	}
}
