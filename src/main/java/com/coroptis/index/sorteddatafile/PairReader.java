package com.coroptis.index.sorteddatafile;

import java.util.Objects;

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
public class PairReader<K, V> implements TypeReader<Pair<K, V>> {

    private final TypeReader<K> keyReader;
    private final TypeReader<V> valueReader;

    public PairReader(final TypeReader<K> keyReader, final TypeReader<V> valueReader) {
	this.keyReader = Objects.requireNonNull(keyReader);
	this.valueReader = Objects.requireNonNull(valueReader);
    }

    @Override
    public Pair<K, V> read(final FileReader reader) {
	final K key = keyReader.read(reader);
	if (key == null) {
	    return null;
	} else {
	    final V value = valueReader.read(reader);
	    return new Pair<K, V>(key, value);
	}
    }

}
