package com.acuumulo.playground;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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

public class AccTable implements AccConstants{

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
			} else writer.close();
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
		
		ColumnVisibility cv = AccAuths.generateColVisIf(auths);		
		if (null == cv) 
			m.put(new Text(cf), new Text(cq), new Value(val.getBytes()));
		else 	
			m.put(new Text(cf), new Text(cq), cv, new Value(val.getBytes()));			
		
		writer.addMutation(m);		
	}
	
	
	/**
	 * Insert row into table.
	 * It is more efficient for multiple writes to use {@link #insertRow(BatchWriter, String, String, String, String, String)}.
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
		
		BatchWriter bw = createBatchWriter(conn, table);		
		try{	
			insertRow(bw, rowid, cf, cq, val, auths);
		} finally {
			closeBatchWriter(bw, false);
		}		
	}
	
	/**
	 * Load csv into table. Table creation must be called outside. 
	 * 
	 * @param conn Connector
	 * @param table String
	 * @param filePath Path
	 * @param defAuths String default auths to use if not provided; may be null
	 * @param stopOnError boolean whether to skip or stop processing bad entries
	 * @return List<Long> of line numbers of any failed mutations.
	 * 
	 * @throws TableNotFoundException 
	 * @throws IOException 
	 * @throws MutationsRejectedException 
	 * 
	 */
	public static List<Long> loadCsv(Connector conn, String table, Path filePath, String defAuths, boolean stopOnError)
					throws TableNotFoundException, IOException, MutationsRejectedException{
        
		logger.info(String.format("<<< processing file '%s' into table '%s' >>>",filePath.toString(),table));
		
		BatchWriter bw = createBatchWriter(conn, table);		
		String dauths = AccAuths.isAuth(defAuths)? defAuths : null;
		List<Long> issues = new ArrayList<>();
		
		try (CSVReader reader = new CSVReader(new FileReader(filePath.toFile()))){		
		
	     String [] line;
	     int len;
	     while ((line = reader.readNext()) != null) {
	    	 len = line.length;
	    	 
	    	 if (reader.getLinesRead() % 10 == 0)
	    		 logger.info(String.format("... now ingesting line #%d (%s) into '%s'",reader.getLinesRead(),filePath.getFileName().toString(),table));
	    	 
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
	    	 
	    	 //Must be at least 4 to use (5 means auths provided)
	    	 if (len >=4){
		         try{
		        	 //set auths (still may be null)
		    		 String auths = len >=5? line[4] : dauths;
		    		 
		    		 //--------------------------------------------------------------
		    		 //insert      rowId	cf			cq			val			auths
		    		 //--------------------------------------------------------------
		    		 insertRow(bw, line[0], line[1], 	line[2], 	line[3],	auths);
		    		 
		         } catch (Exception e){
		        	 issues.add(reader.getLinesRead());
		        	 if (stopOnError){
		        		 logger.warn(String.format("...stopping on issues with mutation at line #%d", reader.getLinesRead()),e);		        		 
		        		 return issues;//terminate
		        	 } else {
		        		 logger.warn(String.format("...skipping mutation at line #%d", reader.getLinesRead()),e);		        		 
		        		 continue;//graceful handling
		        	 }
		         }
	    	 } else if (stopOnError){
	        	 issues.add(reader.getLinesRead());
	        	 logger.warn(String.format("...as directed, stopping on incomplete line #%d", reader.getLinesRead()));	        	 
	        	 return issues;//terminate
	         } else {
	        	 issues.add(reader.getLinesRead());
	        	 logger.warn(String.format("...as directed, skipping incomplete line #%d", reader.getLinesRead()));
	        	 continue;//graceful handling
	         }
	     }
		} finally{			
			closeBatchWriter(bw, stopOnError);			
		}
		
		return issues;
	}
}
