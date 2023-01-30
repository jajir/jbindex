package com.coroptis.index.sorteddatafile;

import com.coroptis.index.Pair;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.TypeReader;

/**
 * Allows read key value pairs from index. Class simply read key value pairs one
 * by one.
 * 
 * @author jajir
 *
 * @param <K>
 * @param <V>
 */
public interface PairTypeReader<K, V> extends TypeReader<Pair<K, V>> {

    /**
     * Define reading as reading of key value pair.
     */
    @Override
    Pair<K, V> read(final FileReader reader);

}
