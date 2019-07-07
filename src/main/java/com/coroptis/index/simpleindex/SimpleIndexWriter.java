package com.coroptis.index.simpleindex;

import java.util.Comparator;

import com.coroptis.index.storage.FileWriter;
import com.coroptis.index.type.TypeArrayWriter;
import com.coroptis.index.type.TypeRawArrayWriter;

public class SimpleIndexWriter<K, V> implements CloseableResource {

    private final TypeArrayWriter<V> valueTypeWriter;

    private final FileWriter writer;

    private final DiffKeyWriter<K> diffKeyWriter;

    private int position;

    public SimpleIndexWriter(final FileWriter fileWriter, final TypeRawArrayWriter<K> keyTypeRawArrayWriter,
	    final Comparator<? super K> keyComparator, final TypeArrayWriter<V> valueTypeArrayWriter) {
	this.writer = fileWriter;
	this.valueTypeWriter = valueTypeArrayWriter;
	diffKeyWriter = new DiffKeyWriter<>(keyTypeRawArrayWriter, keyComparator);
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

	final byte[] valueBytes = valueTypeWriter.toBytes(pair.getValue());
	writer.write(valueBytes);

	int lastPosition = position;
	position = position + diffKeyLength + valueBytes.length;
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
