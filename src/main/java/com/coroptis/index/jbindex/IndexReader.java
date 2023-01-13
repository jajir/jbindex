package com.coroptis.index.jbindex;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.sorteddatafile.Pair;

public class IndexReader<K, V> {

    private final IndexIterator<K, V> indexIterator;
    private Pair<K, V> currentPair;

    IndexReader(final IndexIterator<K, V> indexIterator) {
	this.indexIterator = Objects.requireNonNull(indexIterator);
	tryToReadNextPair();
    }

    public Optional<Pair<K, V>> readCurrent() {
	return Optional.ofNullable(currentPair);
    }

    public void moveToNext() {
	tryToReadNextPair();
    }

    private void tryToReadNextPair() {
	currentPair = indexIterator.next();
    }
}
