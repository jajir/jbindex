package com.coroptis.index.unsorteddatafile;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeWriter;

public class UnsortedDataFileWriter<K, V> implements CloseableResource {

    public final static String STORE = "unsorted.un";

    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final FileWriter fileWriter;

    public UnsortedDataFileWriter(final Directory directory, final String fileName, final TypeWriter<K> keyWriter,
	    final TypeWriter<V> valueWriter) {
	this.keyWriter = Objects.requireNonNull(keyWriter);
	this.valueWriter = Objects.requireNonNull(valueWriter);
	Objects.requireNonNull(directory);
	Objects.requireNonNull(fileName);
	fileWriter = directory.getFileWriter(fileName);
    }

    @Deprecated
    public UnsortedDataFileWriter(final Directory directory, final Class<?> keyClass, final Class<?> valueClass) {
	final TypeConvertors tc = TypeConvertors.getInstance();
	this.fileWriter = Objects.requireNonNull(directory.getFileWriter(STORE));
	Objects.requireNonNull(keyClass);
	this.keyWriter = tc.get(keyClass, OperationType.WRITER);
	Objects.requireNonNull(valueClass);
	this.valueWriter = tc.get(valueClass, OperationType.WRITER);
    }

    public void put(final Pair<K, V> pair) {
	Objects.requireNonNull(pair.getKey());
	Objects.requireNonNull(pair.getValue());
	keyWriter.write(fileWriter, pair.getKey());
	valueWriter.write(fileWriter, pair.getValue());
    }

    @Override
    public void close() {
	fileWriter.close();
    }
}
