package com.coroptis.index.simpledatafile;

import com.coroptis.index.DataFileReader;
import com.coroptis.index.IndexException;
import com.coroptis.index.Pair;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.partiallysorteddatafile.UniqueCache;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileStreamer;

public class SimpleDataFileReader<K, V> implements DataFileReader<K, V> {

    private UniqueCache<K, V> cache;

    SimpleDataFileReader(final ValueMerger<K, V> merger,
            final UnsortedDataFile<K, V> unsortedDataFile) {
        cache = new UniqueCache<>(merger);
        try (final UnsortedDataFileStreamer<K, V> streamer = unsortedDataFile
                .openStreamer()) {
            streamer.stream().forEach(cache::add);
        }
    }

    public Pair<K, V> read() {
        return null;
    }

    @Override
    public void skip(long position) {
        throw new IndexException("Method is not supported.");
    }

    @Override
    public void close() {
        cache.clear();
    }

}
