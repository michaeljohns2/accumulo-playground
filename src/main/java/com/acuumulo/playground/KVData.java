package com.acuumulo.playground;

public class KVData implements AccConstants{

	KVPart kvPart;
	Object data;

	public KVData(KVPart kvPart, Object data){
		this.kvPart = kvPart;
		this.data = data;
	}

	public DataType getExpectedDataType(){
		return kvPart.getDataType();
	}

	public Class<?> getExpectedClass(){
		return kvPart.getDataType().getPartClass();
	}

	public boolean isDataClassExpected(){
		if (data == null) return false;		
		return getExpectedClass().isAssignableFrom(data.getClass());
	}

	/**
	 * Massage data into a String regardless of return type.
	 * 
	 * @param manops Optionally apply manops to results
	 * @return String | null
	 */
	public String getExpectedDataAsString(ManOp... manops){
		String s = null;
		if (isDataClassExpected()){
			switch(getExpectedDataType()){
			case STRING:
				s = (String)data;
				break;
			case LONG:
				s = ((Long)data).toString();
				break;
			case BYTE_ARRAY:
				s = new String((byte[])data);
				break;
			case BOOLEAN:
				s = ((Boolean)data).toString();
				break;
			}
		}
		return AccUtils.manipulate(s, manops);
	}

	public KVPart getKvPart() {
		return kvPart;
	}

	public void setKvPart(KVPart kvPart) {
		this.kvPart = kvPart;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}	
}
