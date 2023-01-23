package com.coroptis.index.unsorteddatafile;

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;

public class UnsortedDataFileSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final PairReader<K, V> pairReader;
    private final FileReader fileReader;

    public UnsortedDataFileSpliterator(final FileReader fileReader,
            final PairReader<K, V> pairReader) {
        this.fileReader = Objects.requireNonNull(fileReader);
        this.pairReader = Objects.requireNonNull(pairReader);
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
    public Spliterator<Pair<K, V>> trySplit() {
        /*
         * It's not supported. So return null.
         */
        return null;
    }

    @Override
    public long estimateSize() {
        /*
         * Stream is not sized. It's not possible to determine stream size.
         */
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL;
    }

}
