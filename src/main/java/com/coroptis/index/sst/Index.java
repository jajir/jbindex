package com.coroptis.index.sst;

public interface Index<K, V> {
    void put(K key, V value);

    V get(K key);

    void delete(K key);
}
