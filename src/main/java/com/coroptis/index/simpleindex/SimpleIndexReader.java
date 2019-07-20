package com.coroptis.index.simpleindex;

import java.util.Comparator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.PairComparator;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

public class SimpleIndexReader<K, V> implements CloseableResource {

    private final PairComparator<K, V> pairComparator;

    private final PairReader<K, V> pairReader;

    public SimpleIndexReader(final FileReader fileReader,
	    final ConvertorFromBytes<K> keyTypeRawArrayReader,
	    final TypeReader<V> valueTypeStreamReader,
	    final Comparator<? super K> keyComparator) {
	pairReader = new PairReader<>(fileReader, keyTypeRawArrayReader, valueTypeStreamReader);
	pairComparator = new PairComparator<>(keyComparator);
    }

    public Pair<K, V> read() {
	return pairReader.read();
    }

    public void skip(final int position) {
	pairReader.skip(position);
    }

    public Stream<Pair<K, V>> stream(final long estimateSize) {
	return StreamSupport.stream(
		new SimpleIndexSpliterator<>(pairReader, pairComparator, estimateSize), false);
    }

    @Override
    public void close() {
	// FIXME close buffere reader.
//	reader.close();
    }

}
