package com.coroptis.index.fileindex;

import com.google.common.base.MoreObjects;

/**
 * Key,value pair object. Pairs are stored in index.
 * 
 * @author jan
 *
 * @param <K>
 * @param <V>
 */
public class Pair<K, V> {

    private final K key;

    private final V value;

    public Pair(final K key, final V value) {
	this.key = key;
	this.value = value;
    }

    @Override
    public String toString() {
	return MoreObjects.toStringHelper(Pair.class).add("key", key).add("value", value)
		.toString();
    }

    public K getKey() {
	return key;
    }

    public V getValue() {
	return value;
    }

}
