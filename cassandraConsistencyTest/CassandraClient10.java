/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package consistencyTests.cassandraConsistencyTest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import consistencyTests.resultFile.TestResultFileWriter;
import consistencyTests.resultFile.TestResultFileWriter.Operation;
import consistencyTests.util.StringToStringMap;

//XXXX if we do replication, fix the consistency levels
/**
 * Cassandra 1.0.6 client for YCSB framework
 */
public class CassandraClient10 extends DB
{
  public static final int Ok = 0;
  public static final int Error = -1;
  public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

  public int ConnectionRetries;
  public int OperationRetries;
  public String column_family;

  private static final String DEFAULT_KEYSPACE_PROPERTY = "keyspace";
  private static final String DEFAULT_KEYSPACE = "usertable";
  private String keyspace;
  
  public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
  public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
  public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
  public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";
 
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY = "cassandra.scanconsistencylevel";
  public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.deleteconsistencylevel";
  public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  Exception errorexception = null;

  List<Mutation> mutations = new ArrayList<Mutation>();
  Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
  Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

  ColumnParent parent;
 
  ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
  ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
  ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;

  private TestResultFileWriter resultWriter;
  private CassandraNodeResolver nodeResolver;
  private Map<String, Client> ipToClient;
  private List<TTransport> trs;
  
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException
  {
    String hosts = getProperties().getProperty("hosts");
    if (hosts == null)
    {
      throw new DBException("Required property \"hosts\" missing for CassandraClient");
    }

    column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
    parent = new ColumnParent(column_family);

    ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
        CONNECTION_RETRY_PROPERTY_DEFAULT));
    OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
        OPERATION_RETRY_PROPERTY_DEFAULT));

    readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
    writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
    scanConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(SCAN_CONSISTENCY_LEVEL_PROPERTY, SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
    deleteConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(DELETE_CONSISTENCY_LEVEL_PROPERTY, DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

    this.keyspace = getProperties().getProperty(DEFAULT_KEYSPACE_PROPERTY, DEFAULT_KEYSPACE);
    
    String[] allhosts = hosts.split(",");
    
    this.ipToClient = new HashMap<String, Cassandra.Client>();
    this.trs = new ArrayList<TTransport>();
    for(String ip : allhosts){
    	Client client = this.createClient(ip);
    	this.ipToClient.put(ip, client);
    }
   
    if(this.resultWriter == null)
    	this.resultWriter = this.getTestResultFileWriter(); 
    this.nodeResolver = new CassandraNodeResolver(allhosts[0]);
  }

	private Client createClient(String ip) throws DBException {
		Exception connectexception = null;
		TTransport tr = null;
		Client client = null;
		for (int retry = 0; retry < ConnectionRetries; retry++) {
			connectexception = null;
			tr = new TFramedTransport(new TSocket(ip, 9160));
			TProtocol proto = new TBinaryProtocol(tr);
			client = new Cassandra.Client(proto);
			try {
				tr.open();
				client.set_keyspace(this.keyspace);
				connectexception = null;
				break;
			} catch (Exception e) {
				connectexception = e;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}
		}
		if (connectexception != null) {
			System.err.println("Unable to connect to " + ip + " after "
					+ ConnectionRetries + " tries");
			throw new DBException(connectexception);
		}
		if(tr == null || client == null)
			throw new DBException("Connection failure to server " + ip);
		this.trs.add(tr);
		return client;
	}
  
	private TestResultFileWriter getTestResultFileWriter() throws DBException{
		String pathToResultFile = getProperties().getProperty("resultfile");
		if(pathToResultFile == null)
			throw new DBException("required property \"resultfile\" missing for CassandraClient");
		return new TestResultFileWriter(pathToResultFile);
	}
  
  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  public void cleanup() throws DBException
  {
    this.closeAllTrs();
    this.resultWriter.close();
    this.resultWriter = null;
  }

  private void closeAllTrs(){
	  for(TTransport tr: this.trs){
		  tr.close();
	  }
  }
  
  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result){
	  throw new UnsupportedOperationException();
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    throw new UnsupportedOperationException();
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int update(String table, String key, HashMap<String, ByteIterator> values)
  {
    return this.executeInsertOrUpdate(Operation.UPDATE, table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int insert(String table, String key, HashMap<String, ByteIterator> values)
  {
	return this.executeInsertOrUpdate(Operation.INSERT, table, key, values);
  }

  private int executeInsertOrUpdate(Operation typeOperation, String table, String key, HashMap<String, ByteIterator> values){
		StringToStringMap expectedValues = new StringToStringMap(values);
		List<String> nodesContainingDataForKey = this.nodeResolver
				.getIpsContainingDate(this.keyspace, this.column_family, key);
		
		String ipForInsertion = nodesContainingDataForKey.get(0);
		Client clientForInsertion = this.ipToClient.get(ipForInsertion);
		
		for (int i = 0; i < OperationRetries; i++) {
			try {
				ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

				Column col;
				ColumnOrSuperColumn column;
				for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
					col = new Column();
					col.setName(ByteBuffer.wrap(entry.getKey()
							.getBytes("UTF-8")));
					String valueAsString = expectedValues.get(entry.getKey());
					col.setValue(ByteBuffer.wrap(valueAsString.getBytes()));
					col.setTimestamp(System.currentTimeMillis());

					column = new ColumnOrSuperColumn();
					column.setColumn(col);

					mutations.add(new Mutation()
							.setColumn_or_supercolumn(column));
				}
				
				mutationMap.put(column_family, mutations);
				record.put(wrappedKey, mutationMap);

				clientForInsertion.batch_mutate(record, writeConsistencyLevel);

				mutations.clear();
				mutationMap.clear();
				record.clear();

				String ipForConsistencyTest = nodesContainingDataForKey.get(1);
				Client clientForConsistencyTest = this.ipToClient
						.get(ipForConsistencyTest);
				long delay = this.getDelayForConsistencyInsertOperation(key,
						expectedValues, clientForConsistencyTest);
				this.resultWriter.write(typeOperation, delay);

				return Ok;
			} catch (Exception e) {
				errorexception = e;
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
  }
  
  private long getDelayForConsistencyInsertOperation(String key, StringToStringMap expectedValues, Client client){
	  long startMillis = System.nanoTime();
	  boolean consistencyReached = false;
	  while(!consistencyReached){
		  StringToStringMap realValues = this.getValueForKey(key, client);
		  consistencyReached = StringToStringMap.doesValuesMatch(expectedValues, realValues);
	  }
	  return System.nanoTime() - startMillis;
  }
  
  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public int delete(String table, String key)
  {
	List<String> nodesContainingDataForKey = this.nodeResolver.getIpsContainingDate(this.keyspace, this.column_family, key);
	String ipForDeletion= nodesContainingDataForKey.get(0);
	Client clientForDeletion = this.ipToClient.get(ipForDeletion);
    for (int i = 0; i < OperationRetries; i++)
    {
      try
      {
        clientForDeletion.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
                      new ColumnPath(column_family),
                      System.currentTimeMillis(),
                      deleteConsistencyLevel);
        
        String ipForConsistencyTest = nodesContainingDataForKey.get(1);
        Client clientForConsistencyTest = this.ipToClient.get(ipForConsistencyTest);
        long delay = this.getDelayConsistencyDeleteOperation(key, clientForConsistencyTest);
        this.resultWriter.write(Operation.DELETE, delay);
        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }
  
  private long getDelayConsistencyDeleteOperation(String key, Client client){
	  long startMillis = System.nanoTime();
	  boolean itemHasBeenRemoved = false;
	  while(!itemHasBeenRemoved){
		  StringToStringMap result = this.getValueForKey(key, client);
		  itemHasBeenRemoved = result.isEmpty();
	  }
	  return System.nanoTime() - startMillis;
  }
  
  public StringToStringMap getValueForKey(String key, Client client)
  {
	Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    for (int i = 0; i < OperationRetries; i++)
    {
      try
      {
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));
        List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, readConsistencyLevel);

        Column column;
        String name;
        ByteIterator value;
        for (ColumnOrSuperColumn oneresult : results)
        {

          column = oneresult.column;
            name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
            value = new ByteArrayByteIterator(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());

          result.put(name,value);
        }
        return new StringToStringMap(result);
      } catch (Exception e)
      {
        errorexception = e;
      }

      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e) { 
    	  // Do nothing
      }
    }
    throw new RuntimeException(errorexception.getMessage());
  }
 
//	public static void main(String[] args) throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException, TException {
//		TFramedTransport tr = new TFramedTransport(new TSocket("127.0.0.1", 2222));
//		TProtocol proto = new TBinaryProtocol(tr);
//	    Client client = new Cassandra.Client(proto);
//		try {
//			tr.open();
//		} catch (TTransportException e) {
//			e.printStackTrace();
//		}
//		client.set_keyspace("usertable");
//		SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));
//		List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap("xyz".getBytes("UTF-8")), new ColumnParent("data"), predicate, ConsistencyLevel.ONE);
//		System.out.println("size: " + results.size());
//	}
}