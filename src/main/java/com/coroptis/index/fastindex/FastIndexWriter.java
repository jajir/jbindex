package com.coroptis.index.fastindex;

import java.util.Comparator;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.cache.UniqueCache;

public class FastIndexWriter<K, V> implements PairWriter<K, V> {

    private final UniqueCache<K, V> cache;

    FastIndexWriter(final ValueMerger<K, V> merger,
            final Comparator<K> keyComparator) {
        this.cache = new UniqueCache<>(keyComparator);
    }

    @Override
    public void close() {
        // It't intentionaly empty.
    }

    @Override
    public void put(final Pair<K, V> pair) {
        cache.put(pair);
    }

}
