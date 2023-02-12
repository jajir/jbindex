package com.coroptis.index;

/**
 * Class represents empty file.
 * 
 * It could be useful to represent empty input.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public class PairReaderEmpty<K, V> implements PairReader<K, V> {

    /**
     * It return just null.
     * 
     * @return Return always <code>null</code>.
     */
    public Pair<K, V> read() {
        return null;
    }

    @Override
    public void close() {
        // Intentionally do nothing
    }

}
