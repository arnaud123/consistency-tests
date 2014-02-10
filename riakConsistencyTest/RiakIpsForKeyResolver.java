package consistencyTests.riakConsistencyTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class RiakIpsForKeyResolver {

	private final String ipToCheckNodesForDataItem;
	private final int replicationFactor;
	private static final String PATH_TO_NODE_RESOLVE_SCRIPT = "/home/ec2-user/getRiakNodesForDataItem.py";
	
	public RiakIpsForKeyResolver(String ipToCheckNodesForDataItem, int replicationFactor){
		if(ipToCheckNodesForDataItem == null)
			throw new IllegalArgumentException("Illegal ip parameter");
		if(replicationFactor < 1)
			throw new IllegalArgumentException("Illegal replicationFactor");
		this.ipToCheckNodesForDataItem = ipToCheckNodesForDataItem;
		this.replicationFactor = replicationFactor;
	}
	
	/*
	 * Returns a list of ip addresses responsible for the data-item 
	 * with the given key. 
	 */
	public List<String> getIpsForKey(String key){
		String output = this.getOutputNodeForDataItemSearch(key);
		return this.parseOutput(output);
	}
	
	/*
	 * Parses the output originating from the process: "/home/ec2-user/getRiakNodesForDataItem.py"
	 * to filter out the ip addresses. 
	 * 
	 * Example toParse string:
	 * 
	 * riak_core_apl:get_apl(chash:key_of(arnaud), 3, riak_kv).Attaching to /var/run/riak/erlang.pipe.1 
	 * (^D to exit) riak_core_apl:get_apl(chash:key_of(arnaud), 3, riak_kv).
	 * [{1004782375664995756265033322492444576013453623296,'riak@172.16.33.2'},
	 * {1027618338748291114361965898003636498195577569280,'riak@172.16.33.4'},
	 * {1050454301831586472458898473514828420377701515264,'riak@172.16.33.5'}] 
	 */
	private List<String> parseOutput(String toParse){
		int startBracket = toParse.indexOf("[");
		toParse = toParse.substring(startBracket+1, toParse.length()-1);
		List<String> result = new ArrayList<String>();
		while(toParse.contains("@")){
			int startIndex = toParse.indexOf("@");
			int endIndex = toParse.indexOf("}");
			String resultItem = toParse.substring(startIndex+1, endIndex-1);
			result.add(resultItem);
			toParse = toParse.substring(Math.min(toParse.length()-1, endIndex+2));
		}
		return result;
	}
	
	/*
	 * Runs the process: "/home/ec2-user/getRiakNodesForDataItem.py"
	 * for the given key and returns the output of the process. 
	 */
	private String getOutputNodeForDataItemSearch(String key){
		String replicationFactorAsString = Integer.toString(this.replicationFactor);
		try {
			String command[] = {"python", PATH_TO_NODE_RESOLVE_SCRIPT, this.ipToCheckNodesForDataItem, replicationFactorAsString, key};
			ProcessBuilder builder = new ProcessBuilder(command);
			builder.redirectErrorStream();
			Process subProcess = builder.start();
			int exitValue = subProcess.waitFor();
			if(exitValue != 0)
				throw new RuntimeException("exitValue subprocess: " + exitValue);
			return this.readFromOutputstream(subProcess);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Could not retrieve nodes for data item for key: " + key);
		}
	}
	
	/*
	 * Read all the output from the given process default inputstream.
	 */
	private String readFromOutputstream(Process process){
		try{
			BufferedReader inputReader = new BufferedReader(
					new InputStreamReader(process.getInputStream()));
			StringBuilder result = new StringBuilder();
			String line = null;
			while ((line = inputReader.readLine()) != null){
				result.append(line);
			}
			inputReader.close();
			return result.toString();
		} catch(IOException exc){
			exc.printStackTrace();
			throw new RuntimeException("Reading from outputStream subprocess failed");
		}
	}
}