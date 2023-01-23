package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;

public class SortedDataFileBuilder<K, V> {

    private Directory directory;

    private String fileName;

    private TypeWriter<V> valueWriter;

    private TypeReader<V> valueReader;

    private Comparator<? super K> keyComparator;

    private ConvertorFromBytes<K> keyConvertorFromBytes;

    private ConvertorToBytes<K> keyConvertorToBytes;

    public SortedDataFileBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public SortedDataFileBuilder<K, V> withFileName(final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public SortedDataFileBuilder<K, V> withValueWriter(final TypeWriter<V> valueWriter) {
        this.valueWriter = Objects.requireNonNull(valueWriter);
        return this;
    }

    public SortedDataFileBuilder<K, V> withValueReader(final TypeReader<V> valueReader) {
        this.valueReader = Objects.requireNonNull(valueReader);
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyComparator(
            final Comparator<? super K> keyComparator) {
        this.keyComparator = Objects.requireNonNull(keyComparator);
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyConvertorFromBytes(
            final ConvertorFromBytes<K> keyConvertorFromBytes) {
        this.keyConvertorFromBytes = Objects.requireNonNull(keyConvertorFromBytes);
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyConvertorToBytes(
            final ConvertorToBytes<K> keyConvertorToBytes) {
        this.keyConvertorToBytes = Objects.requireNonNull(keyConvertorToBytes);
        return this;
    }

    public SortedDataFile<K, V> build() {
        return new SortedDataFile<>(directory, fileName, valueWriter, valueReader, keyComparator,
                keyConvertorFromBytes, keyConvertorToBytes);
    }

}
