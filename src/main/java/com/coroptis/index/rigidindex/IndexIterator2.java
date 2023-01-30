package com.coroptis.index.rigidindex;

import java.util.Iterator;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairFileReader;

public class IndexIterator2<K, V>
        implements Iterator<Pair<K, V>>, CloseableResource {

    private final PairFileReader<K, V> pairFileReader;
    private Pair<K, V> pair;

    public IndexIterator2(final PairFileReader<K, V> pairFileReader) {
        this.pairFileReader = Objects.requireNonNull(pairFileReader);
        tryReadNext();
    }

    @Override
    public boolean hasNext() {
        return pair != null;
    }

    @Override
    public Pair<K, V> next() {
        final Pair<K, V> out = pair;
        tryReadNext();
        return out;
    }

    private void tryReadNext() {
        pair = pairFileReader.read();
    }

    @Override
    public void close() {
        pairFileReader.close();
    }

}
