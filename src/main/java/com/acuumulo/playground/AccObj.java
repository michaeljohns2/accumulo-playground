package com.acuumulo.playground;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AccObj {
	
	final static private Logger logger = Logger.getLogger(AccObj.class);

	private String instanceName;
	private String zookeepers;
	private String rootPassword;

	/**
	 * Constructor
	 * @param instanceName
	 * @param zookeepers
	 * @param rootPassword
	 */
	public AccObj(String instanceName, String zookeepers, String rootPassword) {
		this.instanceName = instanceName;
		this.zookeepers = zookeepers;
		this.rootPassword = rootPassword;
	}

	/**
	 * Get Connector using instance variables.
	 * 
	 * @return Connector
	 * 
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public Connector getConnector()
			throws AccumuloException, AccumuloSecurityException {
		
		//MANAGE LOG NOISE    	
//    	LogManager.getRootLogger().setLevel(Level.INFO);
    	LogManager.getLogger(ClientConfiguration.class).setLevel(Level.ERROR);//Silence benign WARN
		
		Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
		Connector conn = instance.getConnector("root", new PasswordToken(rootPassword));
		return conn;
	}

	/**
	 * ensure root auths are set to those provided. 
	 *     
	 * @param auths List<String>
	 * 
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public void ensureRootAuths(List<String> auths)
			throws AccumuloException, AccumuloSecurityException {
		Connector connector = getConnector();
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations((String[]) auths.toArray()));
	}
}
