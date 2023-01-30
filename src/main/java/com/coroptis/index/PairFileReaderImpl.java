package com.coroptis.index;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.PairTypeReader;

/**
 * Simple implementation of {@link PairFileReader} that uses given
 * {@link PairTypeReader}.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public class PairFileReaderImpl<K, V> implements PairFileReader<K, V> {

    private final FileReader fileReader;
    private final PairTypeReader<K, V> pairReader;

    public PairFileReaderImpl(final Directory directory, final String fileName,
            final PairTypeReader<K, V> pairReader) {
        Objects.requireNonNull(directory);
        Objects.requireNonNull(fileName);
        this.fileReader = directory.getFileReader(fileName);
        this.pairReader = Objects.requireNonNull(pairReader);
    }

    /**
     * Try to read data.
     * 
     * @return Return read data when it's possible. Return <code>null</code>
     *         when there are no data.
     */
    @Override
    public Pair<K, V> read() {
        return pairReader.read(fileReader);
    }

    @Override
    public void skip(final long position) {
        fileReader.skip(position);
    }

    @Override
    public void close() {
        fileReader.close();
    }

}
