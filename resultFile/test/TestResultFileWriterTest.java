package consistencyTests.resultFile.test;

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
import consistencyTests.util.ConsistencyDelayResult;

public class TestResultFileWriterTest {

	@Test
	public void generalTest() throws IOException {
		// Arrange
		Map<Operation, ConsistencyDelayResult> operations = new HashMap<TestResultFileWriter.Operation, ConsistencyDelayResult>();
		operations.put(Operation.INSERT, new ConsistencyDelayResult(123L, 4));
		operations.put(Operation.UPDATE, new ConsistencyDelayResult(6741L, 2));
		operations.put(Operation.DELETE, new ConsistencyDelayResult(547L, 7));
		// Action
		TestResultFileWriter writer = new TestResultFileWriter("/home/arnaud/Desktop/consistency_tmp");
		for(Operation operation : operations.keySet())
			writer.write(operation, operations.get(operation));
		writer.close();
		// Asserts
		Map<Operation, ConsistencyDelayResult> readOperations= new HashMap<TestResultFileWriter.Operation, ConsistencyDelayResult>();
		BufferedReader reader = new BufferedReader(new FileReader(new File("/home/arnaud/Desktop/consistency_tmp")));
		String currentLine;
		while ((currentLine = reader.readLine()) != null) {
			String[] splitted = currentLine.split(",");
			long delay = Long.parseLong(splitted[1]);
			int attempts = Integer.parseInt(splitted[2]);
			ConsistencyDelayResult consistencyResult = new ConsistencyDelayResult(delay, attempts);
			readOperations.put(Operation.valueOf(splitted[0]), consistencyResult);
		}
		reader.close();
		assertTrue(operations.size() == readOperations.size());
		for(Operation operation: operations.keySet()){
			ConsistencyDelayResult expectedResult = operations.get(operation);
			ConsistencyDelayResult realResult = readOperations.get(operation);
			assertTrue(expectedResult.equals(realResult));
		}
	}

}
