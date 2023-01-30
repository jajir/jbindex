package com.coroptis.index.simpledatafile;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.coroptis.index.PairFileReader;
import com.coroptis.index.IndexException;
import com.coroptis.index.Pair;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.partiallysorteddatafile.UniqueCache;
import com.coroptis.index.sorteddatafile.PairComparator;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class CacheSortedReader<K, V> implements PairFileReader<K, V> {

    private List<Pair<K, V>> sortedPairs;

    CacheSortedReader(final ValueMerger<K, V> merger,
            final UnsortedDataFile<K, V> unsortedDataFile,
            final Comparator<K> keyComparator) {
        final UniqueCache<K, V> cache = new UniqueCache<>(merger);
        try (final UnsortedDataFileStreamer<K, V> streamer = unsortedDataFile
                .openStreamer()) {
            streamer.stream().forEach(cache::add);
        }
        final PairComparator<K, V> pairComparator = new PairComparator<>(
                Objects.requireNonNull(keyComparator));
        sortedPairs = cache.getStream().sorted(pairComparator)
                .collect(Collectors.toList());
        cache.clear();
    }

    @Override
    public Pair<K, V> read() {
        if (sortedPairs.isEmpty()) {
            return null;
        } else {
            return sortedPairs.remove(0);
        }
    }

    @Override
    public void skip(long position) {
        throw new IndexException("Method is not supported.");
    }

    @Override
    public void close() {
        sortedPairs.clear();
    }

}
