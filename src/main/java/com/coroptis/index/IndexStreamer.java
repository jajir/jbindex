package com.coroptis.index;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.SortedDataFileSpliterator;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.TypeReader;

public class IndexStreamer<K, V> implements CloseableResource {

    private final PairComparator<K, V> pairComparator;

    private final FileReader fileReader;

    private final long estimateSize;

    private final PairReader<K, V> pairReader;

    IndexStreamer(final Directory directory, final String fileName, final TypeReader<K> keyReader,
	    final TypeReader<V> valueReader, final Comparator<? super K> keyComparator,
	    final long estimateSize) {
	Objects.requireNonNull(valueReader, "Value reader is null");
	pairComparator = new PairComparator<>(keyComparator);
	this.estimateSize = estimateSize;
	pairReader = new PairReader<>(keyReader, valueReader);
	fileReader = directory.getFileReader(fileName);
    }

    public Stream<Pair<K, V>> stream() {
	return StreamSupport.stream(
		new SortedDataFileSpliterator<>(pairReader, fileReader, pairComparator, estimateSize),
		false);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
