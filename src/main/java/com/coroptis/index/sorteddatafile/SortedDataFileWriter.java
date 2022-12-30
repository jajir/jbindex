package com.coroptis.index.sorteddatafile;

import java.util.Comparator;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeWriter;

public class SortedDataFileWriter<K, V> implements CloseableResource {

    private final TypeWriter<V> valueWriter;

    private final FileWriter writer;

    private final DiffKeyWriter<K> diffKeyWriter;

    private int position;

    public SortedDataFileWriter(final FileWriter fileWriter, final ConvertorToBytes<K> keyConvertor,
	    final Comparator<? super K> keyComparator, final TypeWriter<V> valueWriter) {
	this.writer = fileWriter;
	this.valueWriter = valueWriter;
	diffKeyWriter = new DiffKeyWriter<>(keyConvertor, keyComparator);
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

    public int put(final Pair<K, V> pair) {
	return put(pair, false);
    }

    @Override
    public void close() {
	writer.close();
    }

}
