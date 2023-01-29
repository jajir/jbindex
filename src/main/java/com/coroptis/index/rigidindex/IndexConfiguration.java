package com.coroptis.index.rigidindex;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeDescriptor;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

public class IndexConfiguration<K, V> {

    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    public IndexConfiguration(final Directory directory, final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor,
                "Key type descriptor is null.");
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor,
                "Value type descriptor is null.");
    }

    public Directory getDirectory() {
        return directory;
    }

    TypeReader<K> getKeyReader() {
        return keyTypeDescriptor.getTypeReader();
    }

    TypeReader<V> getValueReader() {
        return valueTypeDescriptor.getTypeReader();
    }

    TypeWriter<K> getKeyWriter() {
        return keyTypeDescriptor.getTypeWriter();
    }

    TypeWriter<V> getValueWriter() {
        return valueTypeDescriptor.getTypeWriter();
    }

    Comparator<? super K> getKeyComparator() {
        return keyTypeDescriptor.getComparator();
    };

    ConvertorFromBytes<K> getKeyConvertorFromBytes() {
        return keyTypeDescriptor.getConvertorFromBytes();
    };

    ConvertorToBytes<K> getKeyConvertorToBytes() {
        return keyTypeDescriptor.getConvertorToBytes();
    };

    public UnsortedDataFile<K, V> getUnsortedFile(final String fileName) {
        final UnsortedDataFile<K, V> out = UnsortedDataFile.<K, V>builder()
                .withDirectory(getDirectory()).withFileName(fileName).withKeyReader(getKeyReader())
                .withValueReader(getValueReader()).withKeyWriter(getKeyWriter())
                .withValueWriter(getValueWriter()).build();
        return out;
    }

    public SortedDataFile<K, V> getSortedDataFile(final String fileName) {
        final SortedDataFile<K, V> out = SortedDataFile.<K, V>builder()
                .withDirectory(getDirectory()).withFileName(fileName)
                .withKeyConvertorFromBytes(getKeyConvertorFromBytes())
                .withKeyComparator(getKeyComparator())
                .withKeyConvertorToBytes(getKeyConvertorToBytes()).withValueReader(getValueReader())
                .withValueWriter(getValueWriter()).build();
        return out;
    }

}
