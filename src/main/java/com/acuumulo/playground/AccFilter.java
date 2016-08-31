package com.acuumulo.playground;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class AccFilter {
	
	final private static Logger logger = Logger.getLogger(AccFilter.class);
	
	/**
	 * Filter results by Key matches composed from provided AccFilterObj,
	 * responds to the level of specificity in the provided Key.
	 * @param results Map<Key,Value>
	 * @param afo AccFilterObj
	 * @return Map<Key,Value>
	 */
	public static Map<Key, Value> filterResults(Map<Key, Value> results, AccFilterObj afo){		
		return filterResultsByKeyMatch(results, afo.generateKey());
	}
	
	/**
	 * Filter results by Key matches, responds to the level of specificity in the provided Key.
	 * @param results Map<Key,Value>
	 * @param compKey Key
	 * @return Map<Key,Value>
	 */
	public static Map<Key, Value> filterResultsByKeyMatch(Map<Key, Value> results, Key compKey){
		
		Map<Key,Value> map = new HashMap<>();
		
		if (compKey == null) return map;
		
		//Speed up tests by doing this 1x
				Text tr = compKey.getRow();
				Text tcf = compKey.getColumnFamily();
				Text tcq = compKey.getColumnQualifier();
				Text tcv = compKey.getColumnVisibility();
		
		//Determine which portions to compare based on provided key.
		boolean compRow = tr.getLength() > 0;
		boolean compCf = tcf.getLength() > 0;
		boolean compCq = tcq.getLength() > 0;
		boolean compCv = tcv.getLength() > 0;
		boolean compTs = compKey.getTimestamp() >= 0 && compKey.getTimestamp() < Long.MAX_VALUE;
		
//		logger.debug("...filtering results based on compKey with the following:\n" +
//				"\trow='"+tr+"' comp? "+compRow+" \n" +
//				"\tcf='"+tcf+"' comp? "+compCf+" \n" +
//				"\tcq='"+tcq+"' comp? "+compCq+" \n" +
//				"\tcv='"+tcv+"' comp? "+compCv+" \n" +
//				"\tts='"+compKey.getTimestamp()+"' comp? "+compTs+" \n"
//			   );
													
		for (Entry<Key,Value> entry : results.entrySet()){
			Key key = entry.getKey();
			Value value = entry.getValue();
			
//			logger.debug("testing key: '"+key+"', value: '"+value+"'");
			
			if (compTs && key.getTimestamp() != compKey.getTimestamp()){
//				logger.debug("...no-match (compTs)");
				continue;
			}
			
			if (compRow && key.compareRow(tr) != 0){
//				logger.debug("...no-match (compRow)");
				continue;
			}
			
			if (compCf && key.compareColumnFamily(tcf) != 0){
//				logger.debug("...no-match (compCf)");
				continue;
			}
			
			if (compCq && key.compareColumnQualifier(tcq) != 0){
//				logger.debug("...no-match (compCq)");
				continue;
			}
			
			if (compCv && !isEqual(
					key.getColumnVisibility().copyBytes(),
					compKey.getColumnVisibility().copyBytes())
					){
//				logger.debug("...no-match (compCq)");
				continue;
			}
			
			//if you get here add to filter map
//			logger.debug("...matched!");
			map.put(key, value);
			
		}
		return map;
	}
	
	/**
	 * Filter results by PartialKey matches which may mask select key values.
	 * @param results Map<Key,Value>
	 * @param compKey Key	 
	 * @param partialKey PartialKey
	 * @return Map<Key,Value>
	 */
	public static Map<Key, Value> filterResultsByPartialKeyMatch(Map<Key, Value> results, Key compKey, PartialKey partialKey){
		Map<Key,Value> map = new HashMap<>();
		
		for (Entry<Key,Value> entry : results.entrySet()){
			Key key = entry.getKey();
			Value value = entry.getValue();
			
//			logger.debug("testing key: '"+key+"', value: '"+value+"'");
		
			if (key.equals(compKey,partialKey)){
//				logger.debug("...matched!");
				map.put(key, value);
			}
		}
		
		return map;
	}

	/**
	 * Copied out of {@link Key} class for particular comparisons. 
	 * @param a1 byte[]
	 * @param a2 byte[]
	 * @return boolean
	 */
	private static boolean isEqual(byte a1[], byte a2[]) {
	    if (a1 == a2)
	      return true;

	    int last = a1.length;

	    if (last != a2.length)
	      return false;

	    if (last == 0)
	      return true;

	    // since sorted data is usually compared in accumulo,
	    // the prefixes will normally be the same... so compare
	    // the last two charachters first.. the most likely place
	    // to have disorder is at end of the strings when the
	    // data is sorted... if those are the same compare the rest
	    // of the data forward... comparing backwards is slower
	    // (compiler and cpu optimized for reading data forward)..
	    // do not want slower comparisons when data is equal...
	    // sorting brings equals data together

	    last--;

	    if (a1[last] == a2[last]) {
	      for (int i = 0; i < last; i++)
	        if (a1[i] != a2[i])
	          return false;
	    } else {
	      return false;
	    }

	    return true;

	  }
}
