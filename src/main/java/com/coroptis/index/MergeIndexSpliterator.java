package com.coroptis.index;

import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.store.Merger;

public class MergeIndexSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final IndexReader<K, V> reader1;

    private final IndexReader<K, V> reader2;

    private final Comparator<? super K> keyComparator;

    private final Merger<K, V> merger;

    MergeIndexSpliterator(final IndexReader<K, V> reader1, final IndexReader<K, V> reader2,
	    final Comparator<? super K> keyComparator, final Merger<K, V> merger) {
	this.reader1 = Objects.requireNonNull(reader1);
	this.reader2 = Objects.requireNonNull(reader2);
	this.keyComparator = Objects.requireNonNull(keyComparator);
	this.merger = Objects.requireNonNull(merger);
    }

    @Override
    public int characteristics() {
	return Spliterator.SORTED | Spliterator.NONNULL | Spliterator.DISTINCT;
    }

    @Override
    public long estimateSize() {
	return Long.MAX_VALUE;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> consumer) {
	if (!reader1.readCurrent().isPresent()) {
	    if (reader2.readCurrent().isPresent()) {
		consumer.accept(reader2.readCurrent().get());
		reader2.moveToNext();
		return true;
	    } else {
		return false;
	    }
	}
	final Pair<K, V> p1 = reader1.readCurrent().get();

	if (!reader2.readCurrent().isPresent()) {
	    consumer.accept(p1);
	    reader1.moveToNext();
	    return true;
	}
	final Pair<K, V> p2 = reader2.readCurrent().get();

	final int cmp = keyComparator.compare(p1.getKey(), p2.getKey());
	if (cmp == 0) {
	    // p1 == p2
	    consumer.accept(merger.merge(p1, p2));
	    reader1.moveToNext();
	    reader2.moveToNext();
	} else if (cmp < 0) {
	    // p1 < p2
	    consumer.accept(p1);
	    reader1.moveToNext();
	} else {
	    // p1 > p2
	    consumer.accept(p2);
	    reader2.moveToNext();
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
