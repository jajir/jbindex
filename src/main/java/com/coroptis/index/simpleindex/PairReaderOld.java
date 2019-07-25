package com.coroptis.index.simpleindex;

import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

/**
 * Allows read key value pairs from index. Class allows to skip n-byte to first
 * key value pair.
 * 
 * @author jajir
 *
 * @param <K>
 * @param <V>
 */
@Deprecated
// TODO replace it with PairReader.
public class PairReaderOld<K, V> implements CloseableResource {
    private final TypeReader<V> valueReader;

    private final DiffKeyReader<K> diffKeyReader;

    private final FileReader reader;

    public PairReaderOld(final FileReader fileReader, final ConvertorFromBytes<K> keyConvertor,
	    final TypeReader<V> valueReader) {
	this.reader = fileReader;
	this.valueReader = valueReader;
	this.diffKeyReader = new DiffKeyReader<>(keyConvertor);
    }

    public Pair<K, V> read() {
	final K key = diffKeyReader.read(reader);
	if (key == null) {
	    return null;
	} else {
	    final V value = valueReader.read(reader);
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
