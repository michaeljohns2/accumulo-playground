package com.acuumulo.playground;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

	/**
	 * A builder pattern for Immutable Map (wraps google common).
	 * @author mjohns
	 * 
	 * Example `ImmutableMapBuilder.<String,String>builder().put("test","test").build();`
	 *
	 */
	public class ImmutableMapBuilder {
		
		/**
		 * Wrapper to the com.google.common.collect builder
		 * @return
		 */
		public static <K,V> Builder<K, V> builder(){
			return ImmutableMap.<K, V>builder();
		}
	}
