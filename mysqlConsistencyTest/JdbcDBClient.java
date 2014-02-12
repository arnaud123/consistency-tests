package consistencyTests.mysqlConsistencyTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mysql.jdbc.ReplicationDriver;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import consistencyTests.resultFile.TestResultFileWriter;
import consistencyTests.resultFile.TestResultFileWriter.Operation;
import consistencyTests.util.StringToStringMap;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 * 
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 * 
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type VARCHAR. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 * 
 * <p>
 * The following options must be passed when using this database client.
 * 
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 * 
 * @author sudipto
 * 
 */
public class JdbcDBClient extends DB implements JdbcDBClientConstants {

	private Connection connection;
	private String tableName = "usertable";
	private boolean initialized = false;
	private Properties props;
	private static final String DEFAULT_PROP = "";
	private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
	private TestResultFileWriter resultWriter;
	
	private static class StatementType 	{

		enum OperationType {
			INSERT(1), DELETE(2), READ(3), UPDATE(4), SCAN(5);
			int internalType;

			private OperationType(int type) {
				internalType = type;
			}

			int getHashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + internalType;
				return result;
			}
		}

		OperationType operationType;
		int numFields;

		StatementType(OperationType type, int numFields) {
			this.operationType = type;
			this.numFields = numFields;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.numFields;
			result = prime * result + ((this.operationType == null) ? 0 : this.operationType.getHashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StatementType other = (StatementType) obj;
			if (this.numFields != other.numFields)
				return false;
			if (this.operationType != other.operationType)
				return false;
			return true;
		}
	}

	private TestResultFileWriter getTestResultFileWriter() throws DBException{
		String pathToResultFile = getProperties().getProperty("resultfile");
		if(pathToResultFile == null)
			throw new DBException("required property \"resultfile\" missing for JdbcDBClient");
		return new TestResultFileWriter(pathToResultFile);
	}
	
	/**
	 * Initialize the database connection and set it up for sending requests to
	 * the database. This must be called once per client.
	 * 
	 * @throws
	 */
	@Override
	public void init() throws DBException {
		if (initialized) {
			System.err.println("Client connection already initialized.");
			return;
		}
		/*
		 * Retrieves parameters passed on the command line
		 */
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		//String driverName = props.getProperty(DRIVER_CLASS);

		try {
			ReplicationDriver driver = new ReplicationDriver();

			Properties propsForConnection = new Properties();
			// Automatic failover for slave nodes
			propsForConnection.put("autoReconnect", "true");
			// Load balance between the slaves
			propsForConnection.put("roundRobinLoadBalance", "true");
			propsForConnection.put("user", user);
			propsForConnection.put("password", passwd);

			this.connection = driver.connect(urls, propsForConnection);

			/*
			 * read-only = true => read operation is going to be executed =>
			 * Load balance to the slaves read-only = false => update operation
			 * is going to be executed => Send to operation to the master node
			 */
			this.connection.setReadOnly(false);
			this.connection.setAutoCommit(true);

			this.cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
		} catch (SQLException e) {
			System.err.println("Error in database operation: " + e);
			throw new DBException(e);
		} catch (NumberFormatException e) {
			System.err.println("Invalid value for fieldcount property. " + e);
			throw new DBException(e);
		}
		initialized = true;
		this.resultWriter = this.getTestResultFileWriter();
	}

	@Override
	public void cleanup() throws DBException {
		try {
			this.connection.close();
		} catch (SQLException e) {
			System.err.println("Error in closing the database connection");
			throw new DBException(e);
		}
		this.resultWriter.close();
	}

	private PreparedStatement createAndCacheInsertStatement(
			StatementType insertType, String key) throws SQLException {
		StringBuilder insert = new StringBuilder("INSERT INTO ");
		insert.append(this.tableName);
		insert.append(" VALUES(?");
		for (int i = 0; i < insertType.numFields; i++) {
			insert.append(",?");
		}
		insert.append(");");
		PreparedStatement insertStatement = this.connection
				.prepareStatement(insert.toString());
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(insertType,
				insertStatement);
		if (stmt == null)
			return insertStatement;
		else
			return stmt;
	}

	private PreparedStatement createAndCacheReadStatement(
			StatementType readType, String key) throws SQLException {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(this.tableName);
		read.append(" WHERE ");
		read.append(PRIMARY_KEY);
		read.append(" = ");
		read.append("?;");
		PreparedStatement readStatement = this.connection.prepareStatement(read
				.toString());
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(readType,
				readStatement);
		if (stmt == null)
			return readStatement;
		else
			return stmt;
	}

	private PreparedStatement createAndCacheDeleteStatement(
			StatementType deleteType, String key) throws SQLException {
		StringBuilder delete = new StringBuilder("DELETE FROM ");
		delete.append(this.tableName);
		delete.append(" WHERE ");
		delete.append(PRIMARY_KEY);
		delete.append(" = ?;");
		PreparedStatement deleteStatement = this.connection
				.prepareStatement(delete.toString());
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(deleteType,
				deleteStatement);
		if (stmt == null)
			return deleteStatement;
		else
			return stmt;
	}

	private PreparedStatement createAndCacheUpdateStatement(
			StatementType updateType, String key, StringToStringMap updates) throws SQLException {
		StringBuilder update = new StringBuilder("UPDATE ");
		update.append(this.tableName);
		update.append(" SET ");
		
		/////////////////////////////////////////////////
		int counter = 0;
		for(String updatefield : updates.keySet()){
			String databaseField = this.convertToDatabaseField(updatefield);
			
			update.append(databaseField);
			update.append("=?");
			if(counter < updates.size()-1){
				update.append(", ");
				counter++;
			}
		}
//		for (int i = 1; i <= updateType.numFields; i++) {
//			update.append(COLUMN_PREFIX);
//			update.append(i);
//			update.append("=?");
//			if (i < updateType.numFields)
//				update.append(", ");
//		}
		/////////////////////////////////////////////////
		
		update.append(" WHERE ");
		update.append(PRIMARY_KEY);
		update.append(" = ?;");
		PreparedStatement insertStatement = this.connection
				.prepareStatement(update.toString());
		
		return insertStatement;
		
//		PreparedStatement stmt = this.cachedStatements.putIfAbsent(updateType,
//				insertStatement);
//		if (stmt == null)
//			return insertStatement;
//		else
//			return stmt;
	}

	@Override
	public int read(String tableName, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int scan(String tableName, String startKey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int update(String tableName, String key,
			HashMap<String, ByteIterator> values) {
		
//		for(String bla : values.keySet())
//			System.err.println("bla: " + bla);
		
//		System.err.println("update operation");
		// execute conversion to strings before executing the actual update operation
		StringToStringMap expectedValues = new StringToStringMap(values);
		
		////
//		try {
//			this.listValues(key);
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		for(String f : expectedValues.keySet())
//			System.err.println(f + "---" + expectedValues.get(f));
		////
		
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		try {
			int numFields = values.size();
			StatementType type = new StatementType(
					StatementType.OperationType.UPDATE, numFields);
			PreparedStatement updateStatement = this.cachedStatements.get(type);
			if (updateStatement == null) {
				updateStatement = createAndCacheUpdateStatement(type, key, expectedValues);
			}
			int index = 1;
			
			///////////////////////////////////////////////////////////////////////
			for(int i=0; i<10; i++){
				String currentValue = expectedValues.get(COLUMN_PREFIX.toLowerCase() + i);
				if(currentValue != null)
					updateStatement.setString(index++, currentValue);
			}
//			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
//				System.err.println("Updating values: " + entry.getValue().toString());
//				updateStatement.setString(index++, entry.getValue().toString());
//			}
			updateStatement.setString(index, key);
			
			///////////
			//System.err.println("QUERY: " + updateStatement.toString());
			///////////
			
			int result = updateStatement.executeUpdate();
			if (result != 1)
				return 1;
			long delay = this.getDelayForConsistencyNewValue(key, expectedValues);
			this.resultWriter.write(Operation.UPDATE, delay);
			return SUCCESS;
		} catch (SQLException e) {
			System.err.println("Error in processing update to table: "
					+ tableName + e);
			return -1;
		}
	}

	@Override
	public int insert(String tableName, String key,
			HashMap<String, ByteIterator> values) {
		//System.err.println("insert operation: key=" + key);
		StringToStringMap expectedValues = new StringToStringMap(values);
		
		////
//		try {
//			this.listValues(key);
//		} catch (SQLException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		for(String k : expectedValues.keySet())
//			System.err.println(k + "---" + expectedValues.get(k));
		////
		
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		try {
			int numFields = values.size();
			StatementType type = new StatementType(
					StatementType.OperationType.INSERT, numFields);
			PreparedStatement insertStatement = this.cachedStatements.get(type);
			if (insertStatement == null) {
				insertStatement = createAndCacheInsertStatement(type, key);
			}
			insertStatement.setString(1, key);
			int index = 2;
			
			///////////////////////////////////////////////////
			for(int i=0; i < expectedValues.size(); i++){
				String fieldName = COLUMN_PREFIX.toLowerCase() + i;
				String field = expectedValues.get(fieldName);
				insertStatement.setString(index++, field);
			}
//			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
//				//TODO: fix string notation
//				String field = entry.getValue().toString();
//				System.err.println("Setting field: " + field);
//				insertStatement.setString(index++, field);
//			}
			///////////////////////////////////////////////////
			
			int result = insertStatement.executeUpdate();
			if (result != 1)
				return 1;
			long delay = this.getDelayForConsistencyNewValue(key, expectedValues);
			this.resultWriter.write(Operation.INSERT, delay);
			return SUCCESS;
		} catch (SQLException e) {
			System.err.println("Error in processing insert to table: "
					+ tableName + e);
			return -1;
		}
	}

	private long getDelayForConsistencyNewValue(String key, StringToStringMap expectedValues){
		long startMillis = System.currentTimeMillis();
		boolean newValuesAreConsistent = false;
		//System.err.println("entering new value while loop");
		try {
			while(!newValuesAreConsistent){
				StringToStringMap realValues = this.getItemAsStringToStringMap(key, expectedValues.keySet());
				if(realValues != null && StringToStringMap.doesValuesMatch(expectedValues, realValues))
					newValuesAreConsistent = true;
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error in processing read of table: " + e);
		}
		return System.currentTimeMillis() - startMillis;
	}
	
	private StringToStringMap getItemAsStringToStringMap(String key, Set<String> fields) throws SQLException {
		// Demarcation for read only command to be executed
		StringToStringMap result = new StringToStringMap();
		this.connection.setReadOnly(true);
		StatementType type = new StatementType(
				StatementType.OperationType.READ, 1);
		PreparedStatement readStatement = this.cachedStatements.get(type);
		if (readStatement == null) {
			readStatement = createAndCacheReadStatement(type, key);
		}
		readStatement.setString(1, key);
		ResultSet resultSet = readStatement.executeQuery();
		if (!resultSet.next()) {
			result = null;
		}
		else{
			for(int i=2; i<=11; i++){
				String field = COLUMN_PREFIX.toLowerCase() + (i-2);
				String value = resultSet.getString(i);
				result.put(field, value);
			}
			//////////////////////////////////
//			for (String field : fields) {
//				String value = resultSet.getString(field);
//				result.put(field, value);
//			}
			//////////////////////////////////
		}
		resultSet.close();
		this.connection.setReadOnly(false);
		return result;
	}
	
	@Override
	public int delete(String tableName, String key) {
		System.err.println("delete operation");
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		try {
			StatementType type = new StatementType(
					StatementType.OperationType.DELETE, 1);
			PreparedStatement deleteStatement = cachedStatements.get(type);
			if (deleteStatement == null) {
				deleteStatement = createAndCacheDeleteStatement(type, key);
			}
			deleteStatement.setString(1, key);
			int result = deleteStatement.executeUpdate();
			if (result != 1)
				return 1;
			long delay = this.getDelayForConsistencyDeletion(key);
			this.resultWriter.write(Operation.DELETE, delay);
			return SUCCESS;
		} catch (SQLException e) {
			System.err.println("Error in processing delete to table: "
					+ tableName + e);
			return -1;
		}
	}
	
	private long getDelayForConsistencyDeletion(String key){
		long startMillis = System.currentTimeMillis();
		boolean itemExists = true;
//		System.err.println("entering deletion while loop");
		try {
			while(itemExists){
				itemExists = doesItemExists(key); 
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error in processing read of table: " + e);
		}
		return System.currentTimeMillis() - startMillis;
	}
	
	private boolean doesItemExists(String key) throws SQLException{
		// Demarcation for read only command to be executed
		this.connection.setReadOnly(true);
		StatementType type = new StatementType(
				StatementType.OperationType.READ, 1);
		PreparedStatement readStatement = this.cachedStatements.get(type);
		if (readStatement == null) {
			readStatement = createAndCacheReadStatement(type, key);
		}
		readStatement.setString(1, key);
		ResultSet resultSet = readStatement.executeQuery();
		boolean itemExists = resultSet.next(); 
		resultSet.close();
		this.connection.setReadOnly(false);
		return itemExists;
	}
	
	
	private String convertToDatabaseField(String fieldToConvert){
		String indexAsString = fieldToConvert.substring(5);
		int index = Integer.parseInt(indexAsString);
		return COLUMN_PREFIX.toLowerCase() + (index+1);
	}
	
//	private String convertFromDatabaseField(String fieldToConvert){
//		String indexAsString = fieldToConvert.substring(5);
//		int index = Integer.parseInt(indexAsString);
//		return COLUMN_PREFIX.toLowerCase() + (index-1);
//	}
	
	
	
	
	
	
	
	
	
	
	
	// For testing purposes
	private void listValues(String key) throws SQLException{
		this.connection.setReadOnly(true);
		StatementType type = new StatementType(
				StatementType.OperationType.READ, 1);
		PreparedStatement readStatement = this.cachedStatements.get(type);
		if (readStatement == null) {
			readStatement = createAndCacheReadStatement(type, key);
		}
		readStatement.setString(1, key);
		ResultSet resultSet = readStatement.executeQuery();
		while(resultSet.next()){
			System.err.println("Key: " + resultSet.getString(1));
			for(int i=2; i<=10; i++)
				System.err.println("old field" + (i-2) + ": " + resultSet.getString(i));
		}
		resultSet.close();
		this.connection.setReadOnly(false);
	}
}