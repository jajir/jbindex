package com.coroptis.index;

/**
 * Class represents empty file.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public class PairFileReaderEmpty<K, V> implements PairFileReader<K, V> {

    /**
     * It return just null.
     * 
     * @return Return always <code>null</code>.
     */
    public Pair<K, V> read() {
        return null;
    }

    public void skip(long position) {
        // Intentionally do nothing
    }

    @Override
    public void close() {
        // Intentionally do nothing
    }

}
