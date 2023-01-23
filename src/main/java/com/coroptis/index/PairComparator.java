package com.coroptis.index;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.sorteddatafile.Pair;

public class PairComparator<K, V> implements Comparator<Pair<K, V>> {

    private final Comparator<? super K> keyComparator;

    public PairComparator(final Comparator<? super K> keyComparator) {
        this.keyComparator = Objects.requireNonNull(keyComparator,
                "Key comparator  must not be null");
    }

    @Override
    public int compare(final Pair<K, V> pair1, final Pair<K, V> pair2) {
        return keyComparator.compare(pair1.getKey(), pair2.getKey());
    }

}
