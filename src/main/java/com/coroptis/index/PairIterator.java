package com.coroptis.index;

import java.util.Iterator;
import java.util.Optional;

/**
 * Define key value pair iterator. It allows to go through all records and
 * further works with them. When object is initialized method
 * {@link #readCurrent()} return null.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface PairIterator<K, V>
        extends Iterator<Pair<K, V>>, CloseableResource {

    /**
     * Get currently read key value pair without moving to next.
     * 
     * @return key value pair
     */
    Optional<Pair<K, V>> readCurrent();

}
