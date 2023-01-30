package com.coroptis.index;

/**
 * Resource that store key value pair. Before freeing from memory it requires
 * close method should be called. Close method persists all changes.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface PairFileWriter<K, V> extends CloseableResource {

    void put(Pair<K, V> pair);

}
