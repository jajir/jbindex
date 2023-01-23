package com.coroptis.index.sorteddatafile;

import java.util.Objects;

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

    public static <M, N> Pair<M, N> of(final M m, final N n) {
        return new Pair<M, N>(m, n);
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

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Pair<K, V> other = (Pair<K, V>) obj;
        return Objects.equals(key, other.key) && Objects.equals(value, other.value);
    }

}
