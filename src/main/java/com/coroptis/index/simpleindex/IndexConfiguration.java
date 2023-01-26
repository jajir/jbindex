package com.coroptis.index.simpleindex;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

public class IndexConfiguration<K, V> {

    private final Directory directory;
    private final Class<?> keyClass;
    private final Class<?> valueClass;

    public IndexConfiguration(final Directory directory, final Class<?> keyClass,
            final Class<?> valueClass) {
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
        return null;
    }

    TypeReader<V> getValueReader() {
        return null;
    }

    TypeWriter<K> getKeyWriter() {
        return null;
    }

    TypeWriter<V> getValueWriter() {
        return null;
    }

    Comparator<? super K> getKeyComparator() {
        return null;
    };

    ConvertorFromBytes<K> getKeyConvertorFromBytes() {
        return null;
    };

    ConvertorToBytes<K> getKeyConvertorToBytes() {
        return null;
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
