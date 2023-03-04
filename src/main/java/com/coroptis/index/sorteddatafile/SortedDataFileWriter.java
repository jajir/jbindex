package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeWriter;

public class SortedDataFileWriter<K, V> implements PairWriter<K, V> {

    private final TypeWriter<V> valueWriter;

    private final FileWriter writer;

    private final DiffKeyWriter<K> diffKeyWriter;

    private int position;

    public SortedDataFileWriter(final Directory directory, final String fileName,
            final ConvertorToBytes<K> keyConvertorToBytes,
            final Comparator<K> keyComparator, final TypeWriter<V> valueWriter) {
        Objects.requireNonNull(directory);
        Objects.requireNonNull(fileName);
        this.writer = directory.getFileWriter(fileName);
        this.valueWriter = valueWriter;
        diffKeyWriter = new DiffKeyWriter<>(keyConvertorToBytes, keyComparator);
        position = 0;
    }

    /**
     * Allows to put new key value pair into index.
     *
     * @param pair      required key value pair
     * @param fullWrite when it's <code>true</code> than key is written whole
     *                  without shared part with previous key.
     * @return position of end of record.
     */
    public int put(final Pair<K, V> pair, final boolean fullWrite) {
        final int diffKeyLength = diffKeyWriter.write(writer, pair.getKey(), fullWrite);

        final int writenBytesInValue = valueWriter.write(writer, pair.getValue());

        int lastPosition = position;
        position = position + diffKeyLength + writenBytesInValue;
        return lastPosition;
    }

    @Override
    public void put(final Pair<K, V> pair) {
        put(pair, false);
    }

    @Override
    public void close() {
        writer.close();
    }

}
