package consistencyTests.riakConsistencyTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import consistencyTests.resultFile.TestResultFileWriter;
import consistencyTests.resultFile.TestResultFileWriter.Operation;

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
	
//	private final int maxConnections = 50;
	private Map<String, IRiakClient> clients;
	private RiakIpsForKeyResolver ipForKeyResolver;
	private TestResultFileWriter resultFileWriter;
	
	public RiakClient(){
		this.clients = null;
		this.ipForKeyResolver = null;
		this.resultFileWriter = null;
	}
	
	private String[] getIpAddressesOfNodes() throws DBException {
		String hosts = getProperties().getProperty("hosts");
		if (hosts == null)
			throw new DBException("Required property \"hosts\" missing for RiakClient");
		return hosts.split(",");
	}
	
	private int getReplicationFactor() throws DBException {
		String replicationFactorAsString = getProperties().getProperty("replicationfactor");
		if(replicationFactorAsString == null)
			throw new DBException("required property \"replicationfactor\" missing for riakClient");
		try{
			return Integer.parseInt(replicationFactorAsString);
		} catch(NumberFormatException exc){
			throw new DBException("Replicationfactor must be a strict positive integer");
		}
	}

	private TestResultFileWriter getTestResultFileWriter(){
		String pathToResultFile = getProperties().getProperty("resultfile");
		return new TestResultFileWriter(pathToResultFile);
	}
	
	private Map<String, IRiakClient> getClients() throws DBException{
		Map<String, IRiakClient> result = new HashMap<String, IRiakClient>();
		try {
			for(String ip : getIpAddressesOfNodes()){
				IRiakClient httpClient = RiakFactory.httpClient("http://" + ip + ":8098/riak");
				result.put(ip, httpClient);
			}
		} catch (RiakException e) {
			throw new DBException("Unable to connect to cluster node");
		}
		return result;
	}
	
	@Override
	public void init() throws DBException {
		this.clients = this.getClients();
		int replicationFactor = this.getReplicationFactor();
		// Choose random node in cluster to resolve ip addresses
		this.ipForKeyResolver = new RiakIpsForKeyResolver(getIpAddressesOfNodes()[0], replicationFactor);
		this.resultFileWriter = this.getTestResultFileWriter();
	}
	
	@Override
	public void cleanup() throws DBException {
		if(this.clients != null)
			this.shutdownAllClients();
		this.resultFileWriter.close();
	}
	
	private void shutdownAllClients(){
		if(this.clients == null)
			throw new IllegalStateException("Clients instance is null");
		for(IRiakClient currentClient: this.clients.values()){
			currentClient.shutdown();
		}
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
		System.err.println("update operation");
		List<String> ips = this.ipForKeyResolver.getIpsForKey(key);
		IRiakClient clientForUpdate = this.clients.get(ips.get(0));
		StringToStringMap queryResult = this.executeReadQuery(clientForUpdate, bucketName, key);
		if(queryResult == null)
			return ERROR;
		for(String fieldToUpdate: values.keySet()){
			ByteIterator newValue = values.get(fieldToUpdate);
			queryResult.put(fieldToUpdate, newValue);
		}
		int exitCode = this.executeWriteQuery(clientForUpdate, bucketName, key, queryResult);
		if(exitCode != OK)
			return exitCode;
		long delay = this.checkConsistencyNewValue(ips.get(1), bucketName, key, queryResult);
		this.resultFileWriter.write(Operation.UPDATE, delay);
		return OK;
	}

	@Override
	public int insert(String bucketName, String key,
			HashMap<String, ByteIterator> values) {
		System.err.println("insert operation");
		List<String> ips = this.ipForKeyResolver.getIpsForKey(key);
		IRiakClient clientForInsertion = this.clients.get(ips.get(0));
		StringToStringMap dataToInsert = new StringToStringMap(values);
		int exitCode = this.executeWriteQuery(clientForInsertion, bucketName, key, dataToInsert);
		if(exitCode != OK)
			return exitCode;
		long delay = this.checkConsistencyNewValue(ips.get(1), bucketName, key, dataToInsert);
		this.resultFileWriter.write(Operation.INSERT, delay);
		return OK;
	}

	private long checkConsistencyNewValue(String ip, String bucketName, String key, StringToStringMap expectedValues){
		long startMillis = System.currentTimeMillis();
		IRiakClient client = this.clients.get(ip);
		boolean match = false;
		while(!match){
			StringToStringMap resultMap = this.executeReadQuery(client, bucketName, key);
			if(resultMap != null)
				match = StringToStringMap.doesValuesMatch(expectedValues, resultMap);
		}
		return System.currentTimeMillis() - startMillis;
	}
	
	@Override
	public int delete(String bucketName, String key) {
		System.err.println("delete operation");
		List<String> ips = this.ipForKeyResolver.getIpsForKey(key);
		IRiakClient clientForDeletion = this.clients.get(ips.get(0));
		try {
			Bucket bucket = clientForDeletion.fetchBucket(bucketName).execute();
			DeleteObject delObj = bucket.delete(key);
			delObj.rw(DEFAULT_DELETE_QUORA).execute();
		} catch (RiakRetryFailedException e) {
			return ERROR;
		} catch (RiakException e) {
			return ERROR;
		}
		long delay = this.checkConsistencyDeletion(ips.get(1), bucketName, key);
		this.resultFileWriter.write(Operation.DELETE, delay);
		return OK;
	}
	
	private long checkConsistencyDeletion(String ip, String bucketName, String key){
		long startMillis = System.currentTimeMillis();
		IRiakClient client = this.clients.get(ip);
		StringToStringMap resultMap = null;
		while(resultMap != null){
			resultMap = this.executeReadQuery(client, bucketName, key);
		}
		return System.currentTimeMillis() - startMillis;
	}

}
