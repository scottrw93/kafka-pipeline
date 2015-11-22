package kafka.pipeline.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class DataStore implements Map<Key, Value>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7780027108870133520L;

	private Map<Key, Value> primaryKeyMap;
	

	public DataStore(String... indexes) {
		primaryKeyMap = new HashMap<Key, Value>();
	}

	public int size() {
		return primaryKeyMap.size();
	}

	public boolean isEmpty() {
		return primaryKeyMap.isEmpty();
	}

	public boolean containsKey(Object key) {
		return primaryKeyMap.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return primaryKeyMap.containsValue(value);
	}

	public Value get(Object key) {
		return primaryKeyMap.get(key);
	}

	public Value put(Key key, Value value) {
		return primaryKeyMap.put(key, value);
	}

	public Value remove(Object key) {
		return primaryKeyMap.remove(key);
	}

	public void putAll(Map<? extends Key, ? extends Value> m) {
		primaryKeyMap.putAll(m);		
	}

	public void clear() {
		primaryKeyMap.clear();
	}

	public Set<Key> keySet() {
		return primaryKeyMap.keySet();
	}

	public Collection<Value> values() {
		return primaryKeyMap.values();
	}

	public Set<java.util.Map.Entry<Key, Value>> entrySet() {
		return primaryKeyMap.entrySet();
	}

}
