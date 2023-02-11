package com.coroptis.index;

/**
 * Allows to sequentially read key value pairs from data file.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public interface PairReader<K, V> extends CloseableResource {

    /**
     * Try to read data.
     * 
     * @return Return read data when it's possible. Return <code>null</code>
     *         when there are no data.
     */
    Pair<K, V> read();

}
