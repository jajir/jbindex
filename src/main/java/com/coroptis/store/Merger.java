package com.coroptis.store;

import com.coroptis.index.simpleindex.Pair;

/**
 * Allows to merge two key value pairs into one.
 * 
 * @author jan
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Merger<K, V> {

    default Pair<K, V> merge(Pair<K, V> pair1, Pair<K, V> pair2) {
	final K key = pair1.getKey();
	if (!key.equals(pair2.getKey())) {
	    throw new IllegalArgumentException("Comparing pair with different keys");
	}
	return new Pair<K, V>(key, merge(key, pair1.getValue(), pair2.getValue()));
    }

    /**
     * Merge two values associated with one key.
     * 
     * @param key    required key
     * @param value1 required value of first pair
     * @param value2 required value of second pair
     * @return merged value
     */
    V merge(K key, V value1, V value2);

}
