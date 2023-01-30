package com.coroptis.index.fastindex;

import com.coroptis.index.Pair;
import com.coroptis.index.PairFileWriter;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.partiallysorteddatafile.UniqueCache;

public class FastIndexWriter<K, V> implements PairFileWriter<K, V> {

    private final UniqueCache<K, V> cache;

    FastIndexWriter(final ValueMerger<K, V> merger) {
        this.cache = new UniqueCache<>(merger);
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
