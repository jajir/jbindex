package com.coroptis.index;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.PairReader;

/**
 * Allows to sequentially read key value pairs from data file.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public class DataFileReader<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final PairReader<K, V> pairReader;

    public DataFileReader(final Directory directory, final String fileName,
            final PairReader<K, V> pairReader) {
        Objects.requireNonNull(directory);
        Objects.requireNonNull(fileName);
        this.fileReader = directory.getFileReader(fileName);
        this.pairReader = Objects.requireNonNull(pairReader);
    }

    /**
     * Try to read data.
     * 
     * @return Return read data when it's possible. Return <code>null</code> when
     *         there are no data.
     */
    public Pair<K, V> read() {
        return pairReader.read(fileReader);
    }

    public void skip(final long position) {
        fileReader.skip(position);
    }

    @Override
    public void close() {
        fileReader.close();
    }

}
