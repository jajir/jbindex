package com.coroptis.index;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.PairReader;

/**
 * Allows to use {@link DataFileReader} as {@link Iterator}. Some operations
 * like data merging it makes a lot easier.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class DataFileIterator<K, V>
        implements Iterator<Pair<K, V>>, CloseableResource {

    private final DataFileReaderImpl<K, V> reader;

    private Pair<K, V> current = null;

    public DataFileIterator(final Directory directory, final String fileName,
            final PairReader<K, V> pairReader) {
        this(new DataFileReaderImpl<>(directory, fileName, pairReader));
    }

    public DataFileIterator(final DataFileReaderImpl<K, V> reader) {
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

    @Override
    public void close() {
        reader.close();
    }

}
