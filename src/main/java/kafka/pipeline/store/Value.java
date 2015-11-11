package kafka.pipeline.store;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import kafka.pipeline.serialization.SerializationUtils;


public class Value implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6351950545155343742L;
	private HashMap<String, Object> map;
	
	/**
	 * Values for Kafka producer and consumer, as well as the Value class for the DataStore. Value
	 * can store any number of variables of any type.
	 */
	public Value(){
		map = new HashMap<String, Object>();
	}
	
	/**
	 * Store a value as an object and have is associated with the ID specified
	 * @param id - the id used to retrieve the value.
	 * @param value - the value associated with the id
	 */
	public void put(String id, Object value){
		map.put(id, value);
	}
	
	/**
	 * Retrieve a value associated with the given id.
	 * @param id - the id of the value you wish to retrieve
	 * @return associated with the given id
	 */
	public Object get(String id){
		return map.get(id);
	}

	/**
	 * This method produces a byte stream for use with Kafka producer and consumer.
	 * @param value - the bytes the mehthod is to deserialzie to a Value object
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public static Value deseriailize(byte[] value) throws ClassNotFoundException, IOException {
		return (Value) SerializationUtils.deserialize(value);
	}
	
	/**
	 * This method produces a byte stream for use with Kafka producer and consumer.
	 * @param value - the value object the method is to serialize
	 * @return byte stream of the value object
	 * @throws IOException
	 */
	public static byte[] seriailize(Value value) throws IOException {
		return SerializationUtils.serialize(value);
	}
	
	@Override
	public String toString(){
		return map.toString();
	}
}
