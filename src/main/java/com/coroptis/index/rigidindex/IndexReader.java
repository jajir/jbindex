package com.coroptis.index.rigidindex;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.Pair;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.PairTypeReader;

//FIXME rename to something better it's improved iterator?
public class IndexReader<K, V> {

    private final IndexIterator<K, V> indexIterator;
    private Pair<K, V> currentPair;

    IndexReader(final IndexIterator<K, V> indexIterator) {
        this.indexIterator = Objects.requireNonNull(indexIterator);
        tryToReadNextPair();
    }

    IndexReader(final FileReader reader,
            final PairTypeReader<K, V> pairReader) {
        this.indexIterator = new IndexIterator<>(Objects.requireNonNull(reader),
                Objects.requireNonNull(pairReader));
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
