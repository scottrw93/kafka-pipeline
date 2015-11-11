package kafka.pipeline.store;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import kafka.pipeline.serialization.SerializationUtils;

public class Key implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1615638723804004761L;

	private Object[] keys;
	private int hashcode;

	/**
	 * Key for both messages consumed and produced by Kafka and for use in the datastore provided. It
	 * can consist of any number of objects.
	 * @param keys - Array of objects that make up the key. Allows composite key lookups for datastore.
	 */
	public Key(Object... keys){
		this.keys = keys;
		hashcode = Arrays.hashCode(keys);
	}

	@Override
	public int hashCode(){
		return hashcode;
	}

	@Override
	public boolean equals(Object o){
		if(hashcode == ((Key) o).hashcode)
			return Arrays.equals(keys, ((Key) o).keys);
		return false;
	}

	@Override
	public String toString(){
		return keys.toString();
	}

	/**
	 * This method produces a byte stream for use with Kafka producer and consumer.
	 * @param key - the bytes the mehthod is to deserialzie to a Key object
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public static Key deseriailize(byte[] key) throws ClassNotFoundException, IOException {
		return (Key) SerializationUtils.deserialize(key);
	}

	/**
	 * This method produces a byte stream for use with Kafka producer and consumer.
	 * @param key - the key object the method is to serialize
	 * @return byte stream of the key object
	 * @throws IOException
	 */
	public static byte[] seriailize(Key key) throws IOException {
		return SerializationUtils.serialize(key);
	}
}
