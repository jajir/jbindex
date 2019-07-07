package com.coroptis.index.simpleindex;

import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.coroptis.index.PairComparator;

public class SimpleIndexSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final PairReader<K, V> pairReader;

    private final PairComparator<K, V> pairComparator;

    private final long estimateSize;

    public SimpleIndexSpliterator(final PairReader<K, V> pairReader,
	    final PairComparator<K, V> pairComparator, final long estimateSize) {
	this.pairReader = Objects.requireNonNull(pairReader);
	this.pairComparator = Objects.requireNonNull(pairComparator,
		"pair comparator must not be null");
	this.estimateSize = estimateSize;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> action) {
	final Pair<K, V> out = pairReader.read();
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
	 * Stream is not sized. It's not possible to determine stream size.
	 */
	return estimateSize;
    }

    @Override
    public int characteristics() {
	return Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL
		| Spliterator.SORTED | Spliterator.SIZED;
    }

}
