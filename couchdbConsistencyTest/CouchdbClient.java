package consistencyTests.couchdbConsistencyTest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.UpdateConflictException;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import consistencyTests.resultFile.TestResultFileWriter;
import consistencyTests.resultFile.TestResultFileWriter.Operation;
import consistencyTests.util.StringToStringMap;

/*
 * Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Administrative Contact: dnet-project-office@cs.kuleuven.be
 * Technical Contact: arnaud.schoonjans@student.kuleuven.be
 */
public class CouchdbClient extends DB{
	
	// Default configuration
	private static final String DEFAULT_DATABASE_NAME = "usertable";
	private static final int DEFAULT_COUCHDB_PORT_NUMBER = 5984;
	private static final String PROTOCOL = "http";
	// Database connector
	private List<CouchDbConnector> dbConnectors;
	private int counter;
	// Result file writer
	private TestResultFileWriter resultFileWriter;
	// Return codes
	private static final int OK = 0;
	private static final int UPDATE_CONFLICT = -2;
	private static final int DOC_NOT_FOUND = -3;
	
	public CouchdbClient(){
		this.dbConnectors = null;
		this.counter = 0;
		this.resultFileWriter = null;
	}

//	// Constructor for testing purposes
//	public CouchdbClient(List<URL> urls){
//		if(urls == null)
//			throw new IllegalArgumentException("urls is null");
//		this.dbConnector = new LoadBalancedConnector(urls, DEFAULT_DATABASE_NAME);
//	}
	
	private CouchDbConnector getNextConnector(){
		CouchDbConnector result = this.dbConnectors.get(this.counter);
		this.counter = (this.counter+1)% this.dbConnectors.size();
		return result;
	}
	
	private List<URL> getUrlsForHosts() throws DBException{
		List<URL> result = new ArrayList<URL>();
		String hosts = getProperties().getProperty("hosts");
		String[] differentHosts = hosts.split(",");
		for(String host: differentHosts){
			URL url = this.getUrlForHost(host);
			result.add(url);
		}
		return result;
	}
	
	private URL getUrlForHost(String host) throws DBException{
		String[] hostAndPort = host.split(":");
		try{
			if(hostAndPort.length == 1){
				return new URL(PROTOCOL, host, DEFAULT_COUCHDB_PORT_NUMBER, "");
			} 
			else{
				int portNumber = Integer.parseInt(hostAndPort[1]);
				return new URL(PROTOCOL, hostAndPort[0], portNumber, "");
			}
		} catch(MalformedURLException exc){
			throw new DBException("Invalid host specified");
		} catch(NumberFormatException exc){
			throw new DBException("Invalid port number specified");
		}
	}
	
	private TestResultFileWriter getTestResultFileWriter() throws DBException{
		String pathToResultFile = getProperties().getProperty("resultfile");
		if(pathToResultFile == null)
			throw new DBException("required property \"resultfile\" missing for CouchdbClient");
		return new TestResultFileWriter(pathToResultFile);
	}
	
	@Override
	public void init() throws DBException{
		List<URL> urls = getUrlsForHosts();
		this.dbConnectors = new ArrayList<CouchDbConnector>();
		for(URL url : urls){
			HttpClient httpClient = new StdHttpClient.Builder().url(url).build();
			CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
			CouchDbConnector dbConnector = new StdCouchDbConnector(DEFAULT_DATABASE_NAME, dbInstance);
			this.dbConnectors.add(dbConnector);
		}
		if(this.resultFileWriter == null)
			this.resultFileWriter = this.getTestResultFileWriter();
	}
	
	@Override
	public void cleanup() throws DBException {
		this.resultFileWriter.close();
		this.resultFileWriter = null;
	}
	
	private StringToStringMap executeReadOperation(CouchDbConnector connector, String key){
		try{
			return connector.get(StringToStringMap.class, key);
		} catch(DocumentNotFoundException exc){
			return null;
		}
	}
	
	private int executeWriteOperation(CouchDbConnector connector, String key, StringToStringMap dataToWrite){
		try{
			dataToWrite.put("_id", key);
			connector.create(dataToWrite);
		} catch(UpdateConflictException exc){
			return UPDATE_CONFLICT;
		}
		return OK;
	}
	
	private int executeDeleteOperation(CouchDbConnector connector, StringToStringMap dataToDelete){
		try{
			connector.delete(dataToDelete);
		} catch(UpdateConflictException exc){
			return UPDATE_CONFLICT;
		}
		return OK;
	}
	
	private int executeUpdateOperation(CouchDbConnector connector, StringToStringMap dataToUpdate){
		try{
			connector.update(dataToUpdate);
		} catch(UpdateConflictException exc){
			return UPDATE_CONFLICT;
		}
		return OK;
	}
	
	// Table variable is not used => already contained in database connector
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		throw new UnsupportedOperationException();
	}
	
	// Table variable is not used => already contained in database connector
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		CouchDbConnector connectorForUpdate = this.getNextConnector();
		StringToStringMap queryResult = this.executeReadOperation(connectorForUpdate, key);
		if(queryResult == null)
			return DOC_NOT_FOUND;
		StringToStringMap updatedMap = this.updateFields(queryResult, values);
		int success = this.executeUpdateOperation(connectorForUpdate, updatedMap);
		if(success != OK)
			return success;
		long delay = getDelayForConsistencyNewValue(getNextConnector(), key, updatedMap);
		this.resultFileWriter.write(Operation.UPDATE, delay);
		return OK;
	}

	private StringToStringMap updateFields(StringToStringMap toUpdate, 
							HashMap<String, ByteIterator> newValues){
		for(String updateField: newValues.keySet()){
			ByteIterator newValue = newValues.get(updateField);
			toUpdate.put(updateField, newValue);
		}
		return toUpdate;
	}
	
	// Table variable is not used => already contained in database connector
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		CouchDbConnector connectorForInsert = this.getNextConnector();
		StringToStringMap dataToInsert = new StringToStringMap(values);
		int success = this.executeWriteOperation(connectorForInsert, key, dataToInsert);
		if(success != OK)
			return success;
		long delay = getDelayForConsistencyNewValue(getNextConnector(), key, dataToInsert);
		this.resultFileWriter.write(Operation.INSERT, delay);
		return OK; 
	}

	// Table variable is not used => already contained in database connector
	@Override
	public int delete(String table, String key) {
		CouchDbConnector connectorForDeletion = this.getNextConnector();
		StringToStringMap toDelete = this.executeReadOperation(connectorForDeletion, key);
		if(toDelete == null)
			return DOC_NOT_FOUND;
		int success = this.executeDeleteOperation(connectorForDeletion, toDelete);
		if(success != OK)
			return success;
		long delay = getDelayForConsistencyDeletion(getNextConnector(), key);
		this.resultFileWriter.write(Operation.DELETE, delay);
		return OK;
	}	
	
	private long getDelayForConsistencyNewValue(CouchDbConnector connector, String key, StringToStringMap expectedValues){
		long startMillis = System.currentTimeMillis();
		boolean match = false;
		while(!match){
			StringToStringMap realValues = this.executeReadOperation(connector, key);
			if(realValues != null && StringToStringMap.doesValuesMatch(expectedValues, realValues))
				match = true;
		}
		return System.currentTimeMillis() - startMillis;
	}
	
	private long getDelayForConsistencyDeletion(CouchDbConnector connector, String key){
		long startMillis = System.currentTimeMillis();
		StringToStringMap deleted = null; 
		do{
			deleted = this.executeReadOperation(connector, key);
		} while(deleted != null);
		return System.currentTimeMillis() - startMillis;
	}
}