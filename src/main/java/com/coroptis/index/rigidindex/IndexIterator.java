package com.coroptis.index.rigidindex;

import java.util.Iterator;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.PairTypeReader;
import com.coroptis.index.sorteddatafile.PairTypeReaderImpl;
import com.coroptis.index.type.TypeReader;

public class IndexIterator<K, V>
        implements Iterator<Pair<K, V>>, CloseableResource {

    private final FileReader reader;
    private final PairTypeReader<K, V> pairReader;
    private Pair<K, V> pair;

    public IndexIterator(final FileReader reader, final TypeReader<K> keyReader,
            final TypeReader<V> valueReader) {
        this.reader = Objects.requireNonNull(reader);
        Objects.requireNonNull(keyReader);
        Objects.requireNonNull(valueReader);
        pairReader = new PairTypeReaderImpl<>(keyReader, valueReader);
        tryReadNext();
    }

    public IndexIterator(final FileReader reader,
            final PairTypeReader<K, V> pairReader) {
        this.reader = Objects.requireNonNull(reader);
        this.pairReader = Objects.requireNonNull(pairReader);
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
        pair = pairReader.read(reader);
    }

    @Override
    public void close() {
        reader.close();
    }

}
