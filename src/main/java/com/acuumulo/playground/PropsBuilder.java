package com.acuumulo.playground;

import java.util.Properties;

/**
 * A builder pattern for Properties.
 * @author mjohns
 *
 */
public class PropsBuilder {
	
	public static PropsBuilder init(){
		return new PropsBuilder();
	}	
	
	private Properties props;
		
	public PropsBuilder(){
		props = new Properties();
	}
	
	public PropsBuilder add(String key, String value){
		props.setProperty(key, value);
		return this;
	}
	
	public Properties getProps() {
		return props;
	}

	public PropsBuilder setProps(Properties props) {
		this.props = props;
		return this;
	}	
}
