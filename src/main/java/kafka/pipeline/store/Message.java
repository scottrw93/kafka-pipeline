package kafka.pipeline.store;


public class Message {
	
	private Key key;
	private Value value;
	
	/**
	 * Abstraction of messages recived from Kafka consisting of the Key and Value. This is
	 * for convineance as opposed to neccesity.
	 * @param key
	 * @param value
	 */
	public Message(Key key, Value value) {
		this.key = key;
		this.value = value;
	}
	
	/**
	 * @return Key stored by this message
	 */
	public Key key(){
		return key;
	}
	
	/**
	 * @return Value stored by this message
	 */
	public Value value(){
		return value;
	}
}
