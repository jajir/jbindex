package com.coroptis.index.simpleindex;

import java.util.Comparator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.PairComparator;
import com.coroptis.index.storage.FileReader;
import com.coroptis.index.type.TypeRawArrayReader;
import com.coroptis.index.type.TypeStreamReader;

public class SimpleIndexReader<K, V> implements CloseableResource {

    private final TypeStreamReader<V> valueTypeReader;

    private final DiffKeyReader<K> diffKeyReader;

    private final FileReader reader;

    private final PairComparator<K, V> pairComparator;

    public SimpleIndexReader(final FileReader fileReader, final TypeRawArrayReader<K> keyTypeRawArrayReader,
	    final TypeStreamReader<V> valueTypeStreamReader, final Comparator<? super K> keyComparator) {
	this.reader = fileReader;
	this.valueTypeReader = valueTypeStreamReader;
	this.diffKeyReader = new DiffKeyReader<>(keyTypeRawArrayReader);
	pairComparator = new PairComparator<>(keyComparator);
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

    public void seek(final int position) {
	reader.seek(position);
    }

    public Stream<Pair<K, V>> stream(final long estimateSize) {
	return StreamSupport.stream(new SimpleIndexSpliterator<>(this, pairComparator, estimateSize), false);
    }

    @Override
    public void close() {
	reader.close();
    }

}
