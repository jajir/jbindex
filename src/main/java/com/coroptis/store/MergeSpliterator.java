package com.coroptis.store;

import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFileIterator;
import com.coroptis.index.sorteddatafile.SortedDataFileReader;

public class MergeSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final SortedDataFileIterator<K, V> reader1;

    private final SortedDataFileIterator<K, V> reader2;

    private final Comparator<? super K> keyComparator;

    private final Merger<K, V> merger;

    MergeSpliterator(final SortedDataFileReader<K, V> reader1, final SortedDataFileReader<K, V> reader2,
	    final Comparator<? super K> keyComparator, final Merger<K, V> merger) {
	this.reader1 = new SortedDataFileIterator<K, V>(Objects.requireNonNull(reader1));
	this.reader2 = new SortedDataFileIterator<K, V>(Objects.requireNonNull(reader2));
	this.keyComparator = Objects.requireNonNull(keyComparator);
	this.merger = Objects.requireNonNull(merger);
    }

    @Override
    public int characteristics() {
	return Spliterator.SORTED | Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.IMMUTABLE;
    }

    @Override
    public long estimateSize() {
	/*
	 * Size is not known.
	 */
	return Long.MAX_VALUE;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> consumer) {
	if (!reader1.readCurrent().isPresent()) {
	    if (reader2.readCurrent().isPresent()) {
		consumer.accept(reader2.readCurrent().get());
		reader2.next();
		return true;
	    } else {
		return false;
	    }
	}
	final Pair<K, V> p1 = reader1.readCurrent().get();

	if (!reader2.readCurrent().isPresent()) {
	    consumer.accept(p1);
	    reader1.next();
	    return true;
	}
	final Pair<K, V> p2 = reader2.readCurrent().get();

	final int cmp = keyComparator.compare(p1.getKey(), p2.getKey());
	if (cmp == 0) {
	    // p1 == p2
	    consumer.accept(merger.merge(p1, p2));
	    reader1.next();
	    reader2.next();
	} else if (cmp < 0) {
	    // p1 < p2
	    consumer.accept(p1);
	    reader1.next();
	} else {
	    // p1 > p2
	    consumer.accept(p2);
	    reader2.next();
	}
	return true;
    }

    @Override
    public Comparator<? super Pair<K, V>> getComparator() {
	return (pair1, pair2) -> keyComparator.compare(pair1.getKey(), pair2.getKey());
    }

    @Override
    public Spliterator<Pair<K, V>> trySplit() {
	return null;
    }
}
