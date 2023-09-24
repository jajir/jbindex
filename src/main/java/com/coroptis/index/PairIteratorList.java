package com.coroptis.index;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class PairIteratorList<K, V> implements PairIterator<K, V> {

    private final Iterator<Pair<K, V>> iterator;
    private Pair<K, V> currentPair = null;

    public PairIteratorList(final List<Pair<K, V>> list) {
        this(Objects.requireNonNull(list, "List can't be null.").iterator());
    }

    public PairIteratorList(final Iterator<Pair<K, V>> Iterator) {
        this.iterator = Objects.requireNonNull(Iterator,
                "Iterator can't be null.");
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Pair<K, V> next() {
        currentPair = iterator.next();
        return currentPair;
    }

    @Override
    public void close() {
        /*
         * There is nothing to close.
         */
    }

    @Override
    public Optional<Pair<K, V>> readCurrent() {
        if (currentPair == null) {
            return Optional.empty();
        }
        return Optional.of(currentPair);
    }

}
