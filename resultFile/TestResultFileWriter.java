package consistencyTests.resultFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestResultFileWriter {

	private BufferedWriter resultFileWriter; 
	
	public enum Operation {INSERT, UPDATE, DELETE};
	
	public TestResultFileWriter(String pathToResultFile){
		if(pathToResultFile == null)
			throw new IllegalArgumentException("Parameter pathToResultFile is null");
		File file = new File(pathToResultFile);
		if(file.isDirectory())
			throw new IllegalArgumentException("Parameter pathToResultFile not a valid path");
		try {
			this.resultFileWriter = new BufferedWriter(new FileWriter(file));
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not create writer to file: " +  pathToResultFile);
		}
	}
	
	public boolean isClosed(){
		return (this.resultFileWriter == null);
	}
	
	public void write(Operation operation, long delay) {
		if(this.isClosed())
			throw new IllegalArgumentException("writer has been closed");
		try {
			this.resultFileWriter.write(operation.toString() + "," + delay + "\n");
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not write to file");
		}
	}
	
	public void close(){
		try {
			this.resultFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not close file");
		}
		this.resultFileWriter = null;
	}
	
}