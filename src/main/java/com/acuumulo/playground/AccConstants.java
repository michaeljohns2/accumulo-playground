package com.acuumulo.playground;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface AccConstants {
	
	public static enum ManOp{
		EMPTY_TO_NULL,NULL_TO_EMPTY,TRIM,TO_LOWERCASE,TO_UPPERCASE
	}
	
	public static enum DataType {
		STRING(String.class),LONG(Long.class),BOOLEAN(Boolean.class),BYTE_ARRAY(byte[].class);
		
		final private Class<?> partClass;
		
		private DataType(Class<?> partClass){
			this.partClass = partClass;
		}

		public Class<?> getPartClass() {
			return partClass;
		}
	}
	
	
	public static enum KVPart {
		ROW_ID(DataType.STRING),COL_FAM(DataType.STRING),COL_QUAL(DataType.STRING),
		COL_VIZ(DataType.STRING),TIME_STAMP(DataType.LONG),DEL(DataType.BOOLEAN),VALUE(DataType.BYTE_ARRAY);
		
		final private DataType dataType;
		
		private KVPart(DataType dataType){
			this.dataType = dataType;
		}

		public DataType getDataType() {
			return dataType;
		}
		
		public static KVData getKVData(Entry<Key,Value> entry,KVPart kvPart){
			return getPart(entry.getKey(),entry.getValue(),kvPart);
		}
		
		public static KVData getPart(Key key,Value value,KVPart kvPart){			
			Object data = null;
			switch(kvPart){
			case ROW_ID:				
				data = key.getRow().toString();//String
			case COL_FAM:				
				data = key.getColumnFamily().toString();//String
			case COL_QUAL:				
				data = key.getColumnQualifier().toString();//String
			case COL_VIZ:				
				data = key.getColumnVisibility().toString();//String
			case TIME_STAMP:				
				data = key.getTimestamp();//long
			case DEL:				
				data = key.isDeleted();//boolean
			case VALUE:				
				data = value.get();//byte[]
			}
			
			return new KVData(kvPart,data);
		}
	}
	
	public static enum ScanQualifier{
		STARTS_WITH, STARTS_WITH_CASE_SENSITIVE, EXACT, EXACT_CASE_SENSITIVE, EMPTY_OR_NULL, NOT_EMPTY_OR_NULL;
	}
}
