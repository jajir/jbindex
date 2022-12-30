package com.coroptis.index.sorteddatafile;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public class SortedDataFileIterator<K, V> implements Iterator<Pair<K, V>> {

    private final SortedDataFileReader<K, V> reader;

    private Pair<K, V> current = null;

    public SortedDataFileIterator(final SortedDataFileReader<K, V> reader) {
	this.reader = Objects.requireNonNull(reader);
	current = reader.read();
    }

    public Optional<Pair<K, V>> readCurrent() {
	return Optional.ofNullable(current);
    }

    @Override
    public boolean hasNext() {
	return current != null;
    }

    @Override
    public Pair<K, V> next() {
	if (current == null) {
	    throw new NoSuchElementException();
	}
	final Pair<K, V> out = current;
	current = reader.read();
	return out;
    }

}
