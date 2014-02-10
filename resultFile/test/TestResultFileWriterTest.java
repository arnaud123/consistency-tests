package consistencyTests.resultFile.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import consistencyTests.resultFile.TestResultFileWriter;
import consistencyTests.resultFile.TestResultFileWriter.Operation;

public class TestResultFileWriterTest {

	@Test
	public void generalTest() throws IOException {
		// Arrange
		Map<Operation, Long> operations = new HashMap<TestResultFileWriter.Operation, Long>();
		operations.put(Operation.INSERT, 123L);
		operations.put(Operation.UPDATE, 6741L);
		operations.put(Operation.DELETE, 547L);
		// Action
		TestResultFileWriter writer = new TestResultFileWriter("/home/arnaud/Desktop/consistency_tmp");
		for(Operation operation : operations.keySet())
			writer.write(operation, operations.get(operation));
		writer.close();
		// Asserts
		Map<Operation, Long> readOperations= new HashMap<TestResultFileWriter.Operation, Long>();
		BufferedReader reader = new BufferedReader(new FileReader(new File("/home/arnaud/Desktop/consistency_tmp")));
		String currentLine;
		while ((currentLine = reader.readLine()) != null) {
			String[] splitted = currentLine.split(",");
			readOperations.put(Operation.valueOf(splitted[0]), Long.parseLong(splitted[1]));
		}
		reader.close();
		assertTrue(operations.size() == readOperations.size());
		for(Operation operation: operations.keySet()){
			long expectedValue = operations.get(operation);
			Long readValue = readOperations.get(operation);
			assertFalse(readValue == null);
			assertTrue(expectedValue == readValue.longValue());
		}
	}

}
