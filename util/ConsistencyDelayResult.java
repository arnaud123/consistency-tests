package consistencyTests.util;

public class ConsistencyDelayResult {

	private final long delay;
	private final int attempts;
	
	public ConsistencyDelayResult(long delay, int attempts){
		this.delay = delay;
		this.attempts = attempts;
	}
	
	public long getDelay(){
		return this.delay;
	}
	
	public int getAmountOfAttempts(){
		return this.attempts;
	}
	
	@Override
	public boolean equals(Object object){
		if(object == null)
			return false;
		if(!(object instanceof ConsistencyDelayResult))
			return false;
		ConsistencyDelayResult otherResult = (ConsistencyDelayResult) object;
		if(this.getDelay() != otherResult.getDelay())
			return false;
		if(this.getAmountOfAttempts() != otherResult.getAmountOfAttempts())
			return false;
		return true;
	}
	
}