package com.coroptis.index.simpleindex;

import com.coroptis.index.storage.FileReader;
import com.coroptis.index.type.TypeRawArrayReader;
import com.coroptis.index.type.TypeStreamReader;

/**
 * Allows read key value pairs from index. Class allows to skip n-byte to first
 * key value pair.
 * 
 * @author jajir
 *
 * @param <K>
 * @param <V>
 */
public class PairReader<K, V> implements CloseableResource {

    private final TypeStreamReader<V> valueTypeReader;

    private final DiffKeyReader<K> diffKeyReader;

    private final FileReader reader;

    public PairReader(final FileReader fileReader,
	    final TypeRawArrayReader<K> keyTypeRawArrayReader,
	    final TypeStreamReader<V> valueTypeStreamReader) {
	this.reader = fileReader;
	this.valueTypeReader = valueTypeStreamReader;
	this.diffKeyReader = new DiffKeyReader<>(keyTypeRawArrayReader);
    }

    public Pair<K, V> read() {
	final K key = diffKeyReader.read(reader);
	if (key == null) {
	    return null;
	} else {
	    final V value = valueTypeReader.read(reader);
	    return new Pair<K, V>(key, value);
	}
    }

    public void skip(final int position) {
	reader.skip(position);
    }

    @Override
    public void close() {
	reader.close();
    }

}
