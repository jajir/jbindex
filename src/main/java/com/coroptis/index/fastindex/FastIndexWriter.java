package com.coroptis.index.fastindex;

import java.util.Comparator;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.partiallysorteddatafile.UniqueCache;

public class FastIndexWriter<K, V> implements PairWriter<K, V> {

    private final UniqueCache<K, V> cache;

    FastIndexWriter(final ValueMerger<K, V> merger, final Comparator<K> keyComparator) {
        this.cache = new UniqueCache<>(merger, keyComparator);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void put(final Pair<K, V> pair) {
        cache.add(pair);
    }

}
