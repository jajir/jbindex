package com.coroptis.index.jbindex;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

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

    TypeReader<K> getKeyReader() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final TypeReader<K> keyReader = tc.get(Objects.requireNonNull(getKeyClass()), OperationType.READER);
	return keyReader;
    }

    TypeReader<V> getValueReader() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final TypeReader<V> keyReader = tc.get(Objects.requireNonNull(getValueClass()), OperationType.READER);
	return keyReader;
    }

    TypeWriter<K> getKeyWriter() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final TypeWriter<K> keyReader = tc.get(Objects.requireNonNull(getKeyClass()), OperationType.WRITER);
	return keyReader;
    }

    TypeWriter<V> getValueWriter() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final TypeWriter<V> keyReader = tc.get(Objects.requireNonNull(getValueClass()), OperationType.WRITER);
	return keyReader;
    }

    Comparator<? super K> getKeyComparator() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final Comparator<? super K> keyComparator = tc.get(Objects.requireNonNull(getKeyClass()),
		OperationType.COMPARATOR);
	return keyComparator;
    };

    ConvertorFromBytes<K> getKeyConvertorFromBytes() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final ConvertorFromBytes<K> keyConvertorFromBytes = tc.get(Objects.requireNonNull(getKeyClass()),
		OperationType.CONVERTOR_FROM_BYTES);
	return keyConvertorFromBytes;
    };

    ConvertorToBytes<K> getKeyConvertorToBytes() {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final ConvertorToBytes<K> keyConvertorToBytes = tc.get(Objects.requireNonNull(getKeyClass()),
		OperationType.CONVERTOR_TO_BYTES);
	return keyConvertorToBytes;
    };

    public UnsortedDataFile<K, V> getUnsortedFile(final String fileName) {
	final UnsortedDataFile<K, V> out = UnsortedDataFile.<K, V>builder().withDirectory(getDirectory())
		.withFileName(fileName).withKeyReader(getKeyReader()).withValueReader(getValueReader())
		.withKeyWriter(getKeyWriter()).withValueWriter(getValueWriter()).build();
	return out;
    }

    public SortedDataFile<K, V> getSortedDataFile(final String fileName) {
	final SortedDataFile<K, V> out = SortedDataFile.<K, V>builder().withDirectory(getDirectory())
		.withFileName(fileName).withKeyConvertorFromBytes(getKeyConvertorFromBytes())
		.withKeyComparator(getKeyComparator()).withKeyConvertorToBytes(getKeyConvertorToBytes())
		.withValueReader(getValueReader()).withValueWriter(getValueWriter()).build();
	return out;
    }

}
