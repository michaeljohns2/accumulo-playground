package com.acuumulo.playground;

import java.io.IOException;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
import org.junit.rules.TemporaryFolder;


/**
 * Manage AccObj.
 * Optionally, set-up a mini cluster for development and testing.
 * 
 * @author mjohns
 *
 */
public class AccInstanceMgr {
	
	final private static Logger logger = Logger.getLogger(AccInstanceMgr.class);
	
	private static MiniAccumuloCluster aCluster;
	private static AccObj aObj;
	private static String rootPassword; 
	private static TemporaryFolder miniClusterFolder;
	
	/**
	 * Get MiniCluster (useful for development and testing).
	 * 
	 * @return MiniAccumuloCluster
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static MiniAccumuloCluster getMiniCluster() throws IOException, InterruptedException{
		logger.info("...getMiniCluster() called.");
		if (aCluster == null){			
			logger.info("...creating new MiniAccumuloCluster.");
			miniClusterFolder = new TemporaryFolder();
			miniClusterFolder.create();
			rootPassword = "apasswordhere";
			MiniAccumuloConfig config = new MiniAccumuloConfig(miniClusterFolder.getRoot(), rootPassword);
			aCluster = new MiniAccumuloCluster(config);
			aCluster.start();
		}
		
		return aCluster;
	}
	
	/**
	 * Get an AccObj instance, also calls {@link #getMiniCluster()}.
	 * 
	 * @param useMini boolean
	 * @return AccObj
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static AccObj getAccObj(boolean useMini) throws IOException, InterruptedException{
		
		if (!useMini) throw new UnsupportedOperationException("...getAccObj currently only supports MiniAccumuloCluster.");
		logger.info("...getAccObj() called.");
		if (aObj == null){
			logger.info("...creating new AccObj");
			AccInstanceMgr.getMiniCluster();
			aObj = new AccObj(aCluster.getInstanceName(), aCluster.getZooKeepers(), rootPassword);
		}
		
		return aObj;
	}
	
	/**
	 * Stop mini cluster.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void stopMiniCluster() throws IOException, InterruptedException{
		try{			
			logger.info("...stopMiniCluster() called.");
			if (aCluster != null){
				logger.info("...stopping running MiniAccumuloCluster.");
				aCluster.stop();
				logger.info(String.format("...deleting miniClusterFolder %s", miniClusterFolder.getRoot().getAbsolutePath()));
				miniClusterFolder.delete();
			} else logger.info("...MiniAccumuloCluster is not running.");
		} finally {
			aCluster = null;			
		}
	}
	
	/**
	 * Stop AccObj instance, also calls {@link #stopMiniCluster()}.
	 * 
	 * @param useMini boolean
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void stopAccObj(boolean useMini) throws IOException, InterruptedException{
		
		if (!useMini) throw new UnsupportedOperationException("...getAccObj currently only supports MiniAccumuloCluster.");
		try{
			logger.info("...stopAccObj() called.");
			AccInstanceMgr.stopMiniCluster();
		} finally {			
			aObj = null;
		}
	}
}
