package com.coroptis.index.rigidindex;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairFileReader;

//FIXME rename to something better it's improved iterator?
public class IndexReader2<K, V> implements CloseableResource {

    private final PairFileReader<K, V> pairFileReader;
    private Pair<K, V> currentPair;

    public IndexReader2(final PairFileReader<K, V> pairFileReader) {
        this.pairFileReader = Objects.requireNonNull(pairFileReader);
        tryToReadNextPair();
    }

    public boolean hasCurrent() {
        return currentPair != null;
    }
    
    public Optional<Pair<K, V>> readCurrent() {
        return Optional.ofNullable(currentPair);
    }

    public Optional<Pair<K, V>> readCurrentAndMoveToNext() {
        final Optional<Pair<K, V>> out = readCurrent();
        tryToReadNextPair();
        return out;
    }

    public void moveToNext() {
        tryToReadNextPair();
    }

    private void tryToReadNextPair() {
        currentPair = pairFileReader.read();
    }

    @Override
    public void close() {
        pairFileReader.close();
    }
}
