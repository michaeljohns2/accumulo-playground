package com.acuumulo.playground;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.opencsv.CSVReader;

public class AccTable {

	final private static Logger logger = Logger.getLogger(AccTable.class);
	
	
	/**
	 * Create table if doesn't exist. Optionally drop and recreate.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param deleteIfPresent boolean
	 * 
	 * @throws TableNotFoundException 
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws TableExistsException 
	 */
	public static void setupTable(Connector conn, String table, boolean deleteIfPresent) 
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException{
		
		TableOperations ops = conn.tableOperations();

	    // delete table if directed
	    if (ops.exists(table) && deleteIfPresent){
	    	logger.info(String.format("...deleting existing table %s",table));
	    	ops.delete(table);
	    }

	    // create if needed
	    if (!ops.exists(table)){
	    	logger.info(String.format("...(re)creating table %s",table));
	    	ops.create(table);
	    }
	    else logger.info(String.format("...table %s already created.",table));
	    
	}
	
	/**
	 * Create a BatchWriter for passing into methods that act on a writer.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @return BatchWriter
	 * 
	 * @throws TableNotFoundException
	 */
	public static BatchWriter createBatchWriter(Connector conn, String table) 
			throws TableNotFoundException{
		 return conn.createBatchWriter(table, new BatchWriterConfig());
	}
	
	/**
	 * Convenience method to close a BatchWriter.
	 * @param writer BatchWriter
	 * @param eatException boolean
	 * @throws MutationsRejectedException
	 */
	public static void closeBatchWriter(BatchWriter writer, boolean eatException) throws MutationsRejectedException{
		if (writer != null){
			if (eatException){
				try{
					writer.close();
				} catch(MutationsRejectedException e){
					logger.warn("(eatException)",e);
				}
			}
			
			else writer.close();
		}
	}
	
	/**
	 * Insert row into table using provided BatchWriter.
	 * 
	 * @param writer BatchWriter	 
	 * @param rowid String 
	 * @param cf String
	 * @param cq String
	 * @param val String
	 * @param auths String
	 * 
	 * @throws MutationsRejectedException
	 */
	public static void insertRow(BatchWriter writer, String rowid, String cf, String cq, String val, String auths)
			throws MutationsRejectedException {		
		
		Mutation m = new Mutation(new Text(rowid));
		if (null == auths) {
			m.put(new Text(cf), new Text(cq), new Value(val.getBytes()));
		} else {			
			m.put(new Text(cf), new Text(cq), new ColumnVisibility(auths), new Value(val.getBytes()));			
		}
		writer.addMutation(m);
	}
	
	/**
	 * Insert row with public / null auths using provided BatchWriter.
	 * 
	 * @param writer BatchWriter	 
	 * @param rowid String 
	 * @param cf String
	 * @param cq String
	 * @param val String
	 * 
	 * @throws MutationsRejectedException
	 */
	public static void insertRowPublic(BatchWriter writer, String rowid, String cf, String cq, String val)
			throws MutationsRejectedException {		
		insertRow(writer,rowid,cf,cq,val,null);
	}
	
	
	/**
	 * Insert row into table.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param rowid String 
	 * @param cf String
	 * @param cq String
	 * @param val String
	 * @param auths String
	 * 
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static void insertRow(Connector conn, String table, String rowid, String cf, String cq, String val, String auths)
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException {		
		BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

		Mutation m = new Mutation(new Text(rowid));
		if (null == auths) {
			m.put(new Text(cf), new Text(cq), new Value(val.getBytes()));
		} else {			
			m.put(new Text(cf), new Text(cq), new ColumnVisibility(auths), new Value(val.getBytes()));			
		}
		bw.addMutation(m);

		bw.close();
	}

	/**
	 * Insert row into table with public / null auths.
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param rowid String
	 * @param cf String
	 * @param cq String
	 * @param val String
	 * 
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public static void insertRowPublic(Connector conn, String table, String rowid, String cf, String cq, String val)
			throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		insertRow(conn, table, rowid, cf, cq, val, null);
	}	
	
	/**
	 * Load csv into table. Table creation must be called outside. 
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param filePath Path
	 * @param defAuths String default auths to use if not provided; may be null
	 * @param stopOnError boolean whether to skip or stop processing bad entries
	 * 
	 * @throws TableNotFoundException 
	 * @throws IOException 
	 * @throws MutationsRejectedException 
	 * 
	 */
	public static void loadCsv(Connector conn, String table, Path filePath, String defAuths, boolean stopOnError)
					throws TableNotFoundException, IOException, MutationsRejectedException{
        
		logger.info(String.format("<<< processing file '%s' into table '%s' >>>",filePath.toString(),table));
		BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
		
		try (CSVReader reader = new CSVReader(new FileReader(filePath.toFile()))){		
		
	     String [] line;
	     int len;
	     while ((line = reader.readNext()) != null) {
	    	 len = line.length;
	    	 
	    	 if (len % 10 == 0)
	    		 logger.info(String.format("... now ingesting line #%d (from %s) into table '%s'",reader.getLinesRead(),filePath.getFileName().toString(),table));
	    	 
	    	 // skip comment line
	    	 if (line[0].trim().startsWith("#")){
//	    		 logger.debug(String.format("...skipping empty comment '%s'",line[0]));
	    		 continue;
	    	 }
	    	 
	    	 // skip empty line
	    	 if (len == 1){
//	    		 logger.debug("...skipping empty line");
	    		 continue;
	    	 }
	    	 
//	         logger.debug(Arrays.asList(line).toString());
	    	 
	    	 Mutation m = new Mutation(new Text(line[0]));
	        
	         //inline auths
	         if (len >= 5)
				m.put(new Text(line[1]), new Text(line[2]), new ColumnVisibility(line[4]), new Value(line[3].getBytes()));
	         
	         //default auths
	         else if (len == 4 && defAuths != null && !defAuths.isEmpty())			
	        	 m.put(new Text(line[1]), new Text(line[2]), new ColumnVisibility(defAuths), new Value(line[3].getBytes()));
	         
	         //no auths
	         else if (len == 4)			
	        	 m.put(new Text(line[1]), new Text(line[2]), new Value(line[3].getBytes()));
	         
	         //incomplete line
	         else if (!stopOnError){
	        	 logger.warn(String.format("...as directed, skipping incomplete line #%d", reader.getLinesRead()));
	        	 continue;
	         } else {
	        	 logger.warn(String.format("...as directed, stopping on incomplete line #%d", reader.getLinesRead()));
	        	 return;
	         }
	         
	         //add mutation
	         try{
	        	 bw.addMutation(m);
	         } catch (Exception e){
	        	 logger.warn(String.format("...unable to add mutation at line #%d", reader.getLinesRead()),e);
	        	 if (stopOnError){
	        		 logger.warn("...as directed, stopping.");
	        		 return;
	        	 } else {
	        		 logger.warn("...as directed, skipping.");
	        		 continue;
	        	 }
	         }
	     }
		} finally{			
			bw.close();			
		}
	}
	
	/**
	 * Load csv into table with default public / null visibility. 
	 * 
	 * @param conn Connector
	 * @param table Path
	 * @param filePath String
	 * @param stopOnError boolean whether to skip or stop processing bad entries
	 * 
	 * @throws TableNotFoundException 
	 * @throws IOException 
	 * @throws MutationsRejectedException 
	 * 
	 */
	public static void loadCsvPublic(Connector conn, String table, Path filePath, boolean stopOnError) 
			throws TableNotFoundException, IOException, MutationsRejectedException{
		loadCsv(conn,table,filePath,null,stopOnError);
	}
}
