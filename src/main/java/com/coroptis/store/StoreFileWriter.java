package com.coroptis.store;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.simpleindex.CloseableResource;
import com.coroptis.index.type.TypeWriter;

public class StoreFileWriter<K, V> implements CloseableResource {

    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final FileWriter fileWriter;

    public StoreFileWriter(final Directory directory, final String fileName,
	    final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter) {
	this.fileWriter = Objects.requireNonNull(directory.getFileWriter(fileName));
	this.keyWriter = Objects.requireNonNull(keyWriter);
	this.valueWriter = Objects.requireNonNull(valueWriter);
    }

    public void put(final K key, final V value) {
	Objects.requireNonNull(key);
	Objects.requireNonNull(value);
	keyWriter.write(fileWriter, key);
	valueWriter.write(fileWriter, value);
    }

    @Override
    public void close() {
	fileWriter.close();
    }

}
