package com.coroptis.index.simpleindex;

import java.util.Comparator;

import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.type.ConvertorToBytes;

public class SimpleIndexWriter<K, V> implements CloseableResource {

    private final ConvertorToBytes<V> valueConvertor;

    private final FileWriter writer;

    private final DiffKeyWriter<K> diffKeyWriter;

    private int position;

    public SimpleIndexWriter(final FileWriter fileWriter, final ConvertorToBytes<K> keyConvertor,
	    final Comparator<? super K> keyComparator, final ConvertorToBytes<V> valueConvertor) {
	this.writer = fileWriter;
	this.valueConvertor = valueConvertor;
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

	final byte[] valueBytes = valueConvertor.toBytes(pair.getValue());
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
