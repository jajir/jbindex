package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 */
public class SegmentWriter<K,V> implements PairWriter<K, V> {

    private final Segment<K,V> segment;
    private final UniqueCache<K, V> cache;

    SegmentWriter(final Segment<K,V> segment,
            final SstFileWriter<K, Integer> writer) {
        this.segment = Objects.requireNonNull(segment);
        this.cache= Objects.requireNonNull(segment.getCache());
    }

    @Override
    public void put(final Pair<K, V> pair) {
        cache.put(pair);
    }

    @Override
    public void close() {
        segment.flushCache();
    }

}
