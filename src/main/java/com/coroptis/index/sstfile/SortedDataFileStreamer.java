package com.coroptis.index.sstfile;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;

public class SortedDataFileStreamer<K, V> implements CloseableResource {

    private final PairComparator<K, V> pairComparator;
    private final PairReader<K, V> pairReader;

    public SortedDataFileStreamer(final PairReader<K, V> pairReader,
            final Comparator<? super K> keyComparator) {
        this.pairReader = Objects.requireNonNull(pairReader);
        pairComparator = new PairComparator<>(keyComparator);
    }

    public Stream<Pair<K, V>> stream() {
        return StreamSupport.stream(
                new SortedDataFileSpliterator<>(pairReader, pairComparator),
                false);
    }

    public Stream<Pair<K, V>> stream(final long estimateSize) {
        return StreamSupport.stream(new SortedDataFileSpliteratorSized<>(
                pairReader, pairComparator, estimateSize), false);
    }

    @Override
    public void close() {
        pairReader.close();
    }

}
