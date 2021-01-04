package com.coroptis.store;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.directory.FileReader;
import com.coroptis.index.simpleindex.Pair;
import com.coroptis.index.simpleindex.PairReader;

public class StoreReader<K, V> {

    private final PairReader<K, V> pairReader;

    private final FileReader fileReader;

    private Pair<K, V> currentPair;

    StoreReader(final PairReader<K, V> pairReader, final FileReader fileReader) {
	this.pairReader = Objects.requireNonNull(pairReader);
	this.fileReader = Objects.requireNonNull(fileReader);
	tryToReadNextPair();
    }

    public Optional<Pair<K, V>> readCurrent() {
	return Optional.ofNullable(currentPair);
    }

    public void moveToNext() {
	tryToReadNextPair();
    }

    private void tryToReadNextPair() {
	currentPair = pairReader.read(fileReader);
    }

}
