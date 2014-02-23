package consistencyTests.cassandraConsistencyTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CassandraNodeResolver {

	private final String ip;
	
	public CassandraNodeResolver(String ip){
		if(ip == null)
			throw new IllegalArgumentException("Parameter ip is null");
		this.ip = ip;
	}

	public List<String> getIpsContainingDate(String keyspace,
			String columnFamily, String key) {
		String output = this.getProcessOutput(keyspace, columnFamily, key);
		return this.parseOuput(output);
	}

	private List<String> parseOuput(String output) {
		String[] splittedOutput = output.split("\n");
		List<String> result = new ArrayList<String>();
		for (int i = 1; i < splittedOutput.length; i++) {
			String currentSlice = splittedOutput[i];
			if (!currentSlice.equals(""))
				result.add(currentSlice);
		}
		return result;
	}

	private String getProcessOutput(String keyspace, String columnFamily,
			String key) {
		try {
			String keyAsHexString = this.convertStringToHex(key);
			String threeCommands[] = {"ssh", "root@" + this.ip, "nodetool getendpoints " +  keyspace + " " + columnFamily + " " + keyAsHexString};
			ProcessBuilder builder = new ProcessBuilder(threeCommands);
			builder.redirectErrorStream();
			Process subProcess = builder.start();
			subProcess.waitFor();
			return this.getOutput(subProcess);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		throw new RuntimeException("");
	}

	private String getOutput(Process process) throws IOException {
		BufferedReader subProcessInputReader = new BufferedReader(
				new InputStreamReader(process.getInputStream()));
		String output = "";
		String line = null;
		while ((line = subProcessInputReader.readLine()) != null)
			output += (line + "\n");
		subProcessInputReader.close();
		return output;
	}

	private String convertStringToHex(String str) {
		char[] strAsChars = str.toCharArray();
		StringBuffer hex = new StringBuffer();
		for (int i = 0; i < strAsChars.length; i++) {
			hex.append(Integer.toHexString((int) strAsChars[i]));
		}
		return hex.toString();
	}

//	public static void main(String args[]) {
//		CassandraNodeResolver cassandra = new CassandraNodeResolver("172.16.33.16");
//		if (args.length < 3) {
//			System.out.println("Usage: <keyspace> <columnfamily> <key>");
//			System.exit(1);
//		}
//		String keyspace = args[0];
//		String columnFamily = args[1];
//		String key = args[2];
//		List<String> ips = cassandra.getIpsContainingDate(keyspace,
//				columnFamily, key);
//		for (String ip : ips) {
//			System.out.println("Ip: #" + ip + "#");
//		}
//	}

}