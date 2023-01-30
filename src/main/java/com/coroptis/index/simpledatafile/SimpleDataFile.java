package com.coroptis.index.simpledatafile;

import java.util.Objects;

import com.coroptis.index.DataFileReader;
import com.coroptis.index.PairWriter;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Directory.Access;
import com.coroptis.index.directory.Props;
import com.coroptis.index.type.TypeDescriptor;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileWriter;

public class SimpleDataFile<K, V> {

    private final Directory directory;
    private final String fileName;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ValueMerger<K, V> valueMerger;
    private final Props props;

    public SimpleDataFile(final Directory directory, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final ValueMerger<K, V> valueMerger) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.valueMerger = Objects.requireNonNull(valueMerger);
        this.props = new Props(directory, fileName + ".properties");
    }

    public void merge() {

    }

    public DataFileReader<K, V> openReader() {
        SimpleDataFileReader<K, V> reader = new SimpleDataFileReader<>(
                valueMerger, getCacheFile());
        // FIXME finish method
        return null;
    }

    private UnsortedDataFile<K, V> getCacheFile() {
        final UnsortedDataFile<K, V> out = UnsortedDataFile.<K, V>builder()
                .withDirectory(directory).withFileName(getCacheFileName())
                .withKeyReader(keyTypeDescriptor.getTypeReader())
                .withValueReader(valueTypeDescriptor.getTypeReader())
                .withKeyWriter(keyTypeDescriptor.getTypeWriter())
                .withValueWriter(valueTypeDescriptor.getTypeWriter()).build();
        return out;
    }

    /**
     * Writer ignores previously written data. Just append open data writer for
     * new data.
     * 
     * @return
     */
    public PairWriter<K, V> openCacheWriter() {
        final PairWriter<K, V> basePairWriter = new UnsortedDataFileWriter<>(
                directory, getCacheFileName(),
                keyTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeWriter(), Access.APPEND);
        return new PairWriterCountPair<>(basePairWriter, props);
    }

    private String getCacheFileName() {
        return fileName + "cache.unsorted";
    }

}
