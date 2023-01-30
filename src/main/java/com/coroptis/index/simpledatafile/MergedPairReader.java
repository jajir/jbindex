package com.coroptis.index.simpledatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.IndexException;
import com.coroptis.index.Pair;
import com.coroptis.index.PairFileReader;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.rigidindex.IndexReader2;

public class MergedPairReader<K, V> implements PairFileReader<K, V> {

    private final IndexReader2<K, V> iterator1;
    private final IndexReader2<K, V> iterator2;
    private final Comparator<K> keyComparator;
    private final ValueMerger<K, V> valueMerger;

    public MergedPairReader(final PairFileReader<K, V> reader1,
            final PairFileReader<K, V> reader2,
            final ValueMerger<K, V> valueMerger,
            final Comparator<K> keyComparator) {
        this.iterator1 = new IndexReader2<>(reader1);
        this.iterator2 = new IndexReader2<>(reader2);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.valueMerger = Objects.requireNonNull(valueMerger);
    }

    @Override
    public void close() {
        iterator1.close();
        iterator2.close();
    }

    @Override
    public Pair<K, V> read() {
        if (iterator1.hasCurrent()) {
            if (iterator2.hasCurrent()) {
                final Pair<K, V> p1 = iterator1.readCurrent().get();
                final Pair<K, V> p2 = iterator2.readCurrent().get();
                final K k1 = p1.getKey();
                final K k2 = p2.getKey();
                final int cmp = keyComparator.compare(k1, k2);
                if (cmp == 0) {
                    // p1 == p2
                    iterator1.moveToNext();
                    iterator2.moveToNext();
                    return valueMerger.merge(p1, p2);
                } else if (cmp < 0) {
                    // p1 < p2
                    iterator1.moveToNext();
                    return p1;
                } else {
                    // p1 > p2
                    iterator2.moveToNext();
                    return p2;
                }
            } else {
                return iterator1.readCurrentAndMoveToNext().get();
            }
        } else {
            if (iterator2.hasCurrent()) {
                return iterator2.readCurrentAndMoveToNext().get();
            } else {
                return null;
            }
        }
    }

    @Override
    public void skip(long position) {
        throw new IndexException("Method is not supported.");
    }

}
