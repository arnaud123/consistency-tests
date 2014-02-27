package consistencyTests.riakConsistencyTest;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import consistencyTests.resultFile.TestResultFileWriter;
import consistencyTests.resultFile.TestResultFileWriter.Operation;
import consistencyTests.util.ConsistencyDelayResult;
import consistencyTests.util.StringToStringMap;

/*
Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Administrative Contact: dnet-project-office@cs.kuleuven.be
Technical Contact: arnaud.schoonjans@student.kuleuven.be
*/
public class RiakClient extends DB {

	public static final int OK = 0;
	public static final int ERROR = -1;
	private static final Quora DEFAULT_READ_QUORUM = Quora.ONE;
	private static final Quora DEFAULT_WRITE_QUORUM = Quora.ONE;
	private static final Quora DEFAULT_DELETE_QUORA = Quora.ONE;
	
	private final int maxConnections = 50;
	private IRiakClient clientForModifications;
	private IRiakClient clientForConsistencyChecks;
	private TestResultFileWriter resultFileWriter;
	
	public RiakClient(){
		this.clientForModifications = null;
		this.clientForConsistencyChecks = null;
		this.resultFileWriter = null;
	}
	
	private String[] getIpAddressesOfNodes() throws DBException {
		String hosts = getProperties().getProperty("hosts");
		if (hosts == null)
			throw new DBException("Required property \"hosts\" missing for RiakClient");
		return hosts.split(",");
	}

	private TestResultFileWriter getTestResultFileWriter() throws DBException{
		String pathToResultFile = getProperties().getProperty("resultfile");
		if(pathToResultFile == null)
			throw new DBException("required property \"resultfile\" missing for riakClient");
		return new TestResultFileWriter(pathToResultFile);
	}
	
	private HTTPClusterConfig getClusterConfiguration() throws DBException {
		String[] hosts = this.getIpAddressesOfNodes();
		HTTPClusterConfig clusterConfig = new HTTPClusterConfig(
				this.maxConnections);
		HTTPClientConfig httpClientConfig = HTTPClientConfig.defaults();
		clusterConfig.addHosts(httpClientConfig, hosts);
		return clusterConfig;
	}
	
	private IRiakClient createRiakClient() throws DBException{
		HTTPClusterConfig clusterConfig = getClusterConfiguration();
		try {
			return RiakFactory.newClient(clusterConfig);
		} catch (RiakException e) {
			throw new DBException("Unable to connect to cluster nodes");
		}
	}
	
	@Override
	public void init() throws DBException {
		this.clientForModifications = this.createRiakClient();
		this.clientForConsistencyChecks = this.createRiakClient();
		this.resultFileWriter = this.getTestResultFileWriter();
	}
	
	@Override
	public void cleanup() throws DBException {
		this.shutdownAllClients();
		this.resultFileWriter.close();
	}
	
	private void shutdownAllClients(){
		this.shutdownClient(this.clientForModifications);
		this.shutdownClient(this.clientForConsistencyChecks);
	}
	
	private void shutdownClient(IRiakClient client){
		if(client != null)
			client.shutdown();
	}
	
	private StringToStringMap executeReadQuery(IRiakClient client, String bucketName, String key) {
		try {
			Bucket bucket = client.fetchBucket(bucketName).execute();
			FetchObject<StringToStringMap> fetchObj = bucket.fetch(key, StringToStringMap.class);
			return fetchObj.r(DEFAULT_READ_QUORUM).execute();
		} catch (Exception exc) {
			System.out.println("EXCEPTION: " + exc);
			return null;
		}
	}
	
	private int executeWriteQuery(IRiakClient client, String bucketName, String key, StringToStringMap dataToWrite){
		try {
			Bucket bucket = client.fetchBucket(bucketName).execute();
			StoreObject<StringToStringMap> storeObject = bucket.store(key, dataToWrite);
			storeObject.w(DEFAULT_WRITE_QUORUM).execute();
		} catch (Exception e) {
			return ERROR;
		}
		return OK;
	}
	
	@Override
	public int read(String bucketName, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int update(String bucketName, String key,
			HashMap<String, ByteIterator> values) {
		StringToStringMap queryResult = this.executeReadQuery(this.clientForModifications, bucketName, key);
		if(queryResult == null)
			return ERROR;
		for(String fieldToUpdate: values.keySet()){
			ByteIterator newValue = values.get(fieldToUpdate);
			queryResult.put(fieldToUpdate, newValue);
		}
		int exitCode = this.executeWriteQuery(this.clientForModifications, bucketName, key, queryResult);
		if(exitCode != OK)
			return exitCode;
		ConsistencyDelayResult consistencyResult = this.checkConsistencyNewValue(bucketName, key, queryResult);
		this.resultFileWriter.write(Operation.UPDATE, consistencyResult);
		return OK;
	}

	@Override
	public int insert(String bucketName, String key,
			HashMap<String, ByteIterator> values) {
		StringToStringMap dataToInsert = new StringToStringMap(values);
		int exitCode = this.executeWriteQuery(this.clientForModifications, bucketName, key, dataToInsert);
		if(exitCode != OK)
			return exitCode;
		ConsistencyDelayResult consistencyResult = this.checkConsistencyNewValue(bucketName, key, dataToInsert);
		this.resultFileWriter.write(Operation.INSERT, consistencyResult);
		return OK;
	}

	private ConsistencyDelayResult checkConsistencyNewValue(String bucketName, String key, StringToStringMap expectedValues){
		long startNanos = System.nanoTime();
		int attempts = 0;
		boolean match = false;
		while(!match){
			StringToStringMap resultMap = this.executeReadQuery(this.clientForConsistencyChecks, bucketName, key);
			if(resultMap != null)
				match = StringToStringMap.doesValuesMatch(expectedValues, resultMap);
			attempts++;
			// Value already overwritten by other client thread
			if(System.nanoTime() - startNanos > 2000000000L)
				return new ConsistencyDelayResult(2000000000L, attempts);
		}
		long delay = System.nanoTime() - startNanos;
		return new ConsistencyDelayResult(delay, attempts);
	}
	
	@Override
	public int delete(String bucketName, String key) {
		try {
			Bucket bucket = this.clientForModifications.fetchBucket(bucketName).execute();
			DeleteObject delObj = bucket.delete(key);
			delObj.rw(DEFAULT_DELETE_QUORA).execute();
		} catch (RiakRetryFailedException e) {
			return ERROR;
		} catch (RiakException e) {
			return ERROR;
		}
		ConsistencyDelayResult consistencyResult = this.checkConsistencyDeletion(bucketName, key);
		this.resultFileWriter.write(Operation.DELETE, consistencyResult);
		return OK;
	}
	
	private ConsistencyDelayResult checkConsistencyDeletion(String bucketName, String key){
		long startNanos= System.nanoTime();
		int attempts = 0;
		StringToStringMap resultMap = null;
		while(resultMap != null){
			resultMap = this.executeReadQuery(this.clientForConsistencyChecks, bucketName, key);
			attempts++;
			// Value already overwritten by other client thread
			if(System.nanoTime() - startNanos > 2000000000L)
				return new ConsistencyDelayResult(2000000000L, attempts);
		}
		long delay = System.nanoTime() - startNanos;
		return new ConsistencyDelayResult(delay, attempts);
	}

}
