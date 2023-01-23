package com.coroptis.index.partiallysorteddatafile;

import java.util.List;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.DataFileReader;
import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.SortSupport;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFile;

public class PartiallySortedDataFileReader<K, V> implements CloseableResource {

    /**
     * First and default round.
     */
    private final static int ROUND_0 = 0;

    private final BasicIndex<K, V> basicIndex;
    private DataFileReader<K, V> currentReader;
    private final List<String> fileNames;

    public PartiallySortedDataFileReader(final BasicIndex<K, V> basicIndex,
            final SortSupport<K, V> sortSupport) {
        this.basicIndex = Objects.requireNonNull(basicIndex);
        this.fileNames = sortSupport.getFilesInRound(ROUND_0);
        moveToNextFileName();
    }

    /**
     * Try to read data.
     * 
     * @return Return read data when it's possible. Return <code>null</code> when
     *         there are no data.
     */
    public Pair<K, V> read() {
        if (currentReader == null) {
            return null;
        }
        Pair<K, V> out = currentReader.read();
        if (out == null) {
            moveToNextFileName();
            if (currentReader == null) {
                return null;
            }
            return read();
        } else {
            return out;
        }
    }

    public void skip(final long position) {
        throw new IllegalStateException();
    }

    private void moveToNextFileName() {
        tryToCloseCurrentReader();
        if (fileNames.isEmpty()) {
            return;
        }
        final String currentFileName = fileNames.remove(0);
        final SortedDataFile<K, V> dataFile = basicIndex.getSortedDataFile(currentFileName);
        currentReader = dataFile.openReader();
    }

    @Override
    public void close() {
        tryToCloseCurrentReader();
    }

    private void tryToCloseCurrentReader() {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
    }

}
