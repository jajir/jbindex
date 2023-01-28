package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.coroptis.index.directory.FileReader;

public class SortedDataFileSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final PairReader<K, V> pairReader;

    private final FileReader fileReader;

    private final PairComparator<K, V> pairComparator;

    public SortedDataFileSpliterator(final PairReader<K, V> pairReader, final FileReader fileReader,
            final PairComparator<K, V> pairComparator) {
        this.pairReader = Objects.requireNonNull(pairReader);
        this.fileReader = Objects.requireNonNull(fileReader);
        this.pairComparator = Objects.requireNonNull(pairComparator,
                "pair comparator must not be null");
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> action) {
        final Pair<K, V> out = pairReader.read(fileReader);
        if (out == null) {
            return false;
        } else {
            action.accept(out);
            return true;
        }
    }

    @Override
    public Comparator<? super Pair<K, V>> getComparator() {
        return pairComparator;
    }

    @Override
    public Spliterator<Pair<K, V>> trySplit() {
        /*
         * It's not supported. So return null.
         */
        return null;
    }

    @Override
    public long estimateSize() {
        /*
         * Stream is not sized.
         */
        return Integer.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL
                | Spliterator.SORTED;
    }

}
