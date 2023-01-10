package com.coroptis.index;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeReader;

public class IndexConfiguration<K, V> {

    private final Directory directory;
    private final Class<?> keyClass;
    private final Class<?> valueClass;

    public IndexConfiguration(final Directory directory, final Class<?> keyClass, final Class<?> valueClass) {
	this.directory = Objects.requireNonNull(directory);
	this.keyClass = Objects.requireNonNull(keyClass);
	this.valueClass = Objects.requireNonNull(valueClass);
    }

    public Directory getDirectory() {
	return directory;
    }

    public Class<?> getKeyClass() {
	return keyClass;
    }

    public Class<?> getValueClass() {
	return valueClass;
    }

    public TypeReader<K> getKeyReader() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final TypeReader<K> keyReader = tc.get(Objects.requireNonNull(getKeyClass()), OperationType.READER);
	return keyReader;
    }

    public TypeReader<V> getValueReader() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final TypeReader<V> keyReader = tc.get(Objects.requireNonNull(getValueClass()), OperationType.READER);
	return keyReader;
    }

}
