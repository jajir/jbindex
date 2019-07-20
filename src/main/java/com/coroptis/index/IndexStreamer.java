package com.coroptis.index;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.simpleindex.CloseableResource;
import com.coroptis.index.simpleindex.Pair;
import com.coroptis.index.simpleindex.PairReader;
import com.coroptis.index.simpleindex.SimpleIndexSpliterator;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

public class IndexStreamer<K, V> implements CloseableResource {

    private final PairComparator<K, V> pairComparator;

    private final long estimateSize;

    private final PairReader<K, V> pairReader;

    IndexStreamer(final Directory directory, final String fileName,
	    final ConvertorFromBytes<K> keyConvertor, final TypeReader<V> valueReader,
	    final Comparator<? super K> keyComparator, final long estimateSize) {
	Objects.requireNonNull(valueReader, "Value reader is null");
	pairComparator = new PairComparator<>(keyComparator);
	this.estimateSize = estimateSize;
	pairReader = new PairReader<>(directory.getFileReader(fileName), keyConvertor, valueReader);
    }

    public Stream<Pair<K, V>> stream() {
	return StreamSupport.stream(
		new SimpleIndexSpliterator<>(pairReader, pairComparator, estimateSize), false);
    }

    @Override
    public void close() {
	pairReader.close();

    }

}
