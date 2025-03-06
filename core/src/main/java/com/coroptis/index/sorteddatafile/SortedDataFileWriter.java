package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.ConvertorToBytes;

public class SortedDataFileWriter<K, V> implements PairWriter<K, V> {

    private final SeekeableFileWriter writer;

    private final DiffKeyWriter<K> diffKeyWriter;

    private final ConvertorToBytes<V> valueConvertorToBytes;

    public SortedDataFileWriter(final SeekeableFileWriter writer,
            final ConvertorToBytes<K> keyConvertorToBytes,
            final Comparator<K> keyComparator, final ConvertorToBytes<V> valueConvertorToBytes) {
        Objects.requireNonNull(writer);
        this.writer = writer;
        this.valueConvertorToBytes = Objects.requireNonNull(valueConvertorToBytes);
        diffKeyWriter = new DiffKeyWriter<>(keyConvertorToBytes, keyComparator);
    }

    /**
     * Allows to put new key value pair into index.
     *
     * @param pair      required key value pair
     * @param fullWrite when it's <code>true</code> than key is written whole
     *                  without shared part with previous key.
     * @return position of end of record.
     */
    public long put(final Pair<K, V> pair, final boolean fullWrite) {
        diffKeyWriter.write(writer, pair.getKey(),
                fullWrite);
        final long position = writer.flushAndWrite(valueConvertorToBytes.toBytes(pair.getValue()));

        return position;
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
