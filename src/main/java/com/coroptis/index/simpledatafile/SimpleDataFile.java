package com.coroptis.index.simpledatafile;

import java.util.Objects;

import com.coroptis.index.IndexException;
import com.coroptis.index.PairFileReader;
import com.coroptis.index.PairFileWriter;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Directory.Access;
import com.coroptis.index.directory.Props;
import com.coroptis.index.rigidindex.IndexIterator2;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.type.TypeDescriptor;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileWriter;

public class SimpleDataFile<K, V> {

    final static String NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE = "number_of_key_value_pairs_in_main_file";

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
        Objects.requireNonNull(fileName, "File name is required");
        if (fileName.contains(".")) {
            throw new IndexException(String.format(
                    "File name '%s' should not contain any dots.", fileName));
        }
        this.fileName = Objects.requireNonNull(fileName);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.valueMerger = Objects.requireNonNull(valueMerger);
        this.props = new Props(directory, fileName + ".properties");
    }

    public Stats getStats() {
        return new Stats(
                props.getLong(
                        PairWriterCountPair.NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE),
                props.getLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE));
    }

    /**
     * Merge data from cache to main index.
     */
    public void merge() {
        long cx = 0;
        try (final IndexIterator2<K, V> iterator = new IndexIterator2<>(
                openReader())) {
            try (final SortedDataFileWriter<K, V> writer = getTempFile()
                    .openWriter()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                    cx++;
                }
            }
        }
        props.setLong(PairWriterCountPair.NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE,
                0);
        props.setLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE, cx);
        props.writeData();
    }

    /**
     * Return merged and sorted data from cache and main file. Access time to
     * reader could significantly slow down when number of data in cache.
     * 
     * @return reader with all data
     */
    public PairFileReader<K, V> openReader() {
        final CacheSortedReader<K, V> reader1 = new CacheSortedReader<>(
                valueMerger, getCacheFile(), keyTypeDescriptor.getComparator());
        final PairFileReader<K, V> reader2 = getMainFile().openReader();
        final MergedPairReader<K, V> mergedPairReader = new MergedPairReader<>(
                reader1, reader2, valueMerger,
                keyTypeDescriptor.getComparator());
        return mergedPairReader;
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

    private SortedDataFile<K, V> getMainFile() {
        final SortedDataFile<K, V> out = getSortedFile(getSortedFileName());
        return out;
    }

    private SortedDataFile<K, V> getTempFile() {
        final SortedDataFile<K, V> out = getSortedFile(getMergedFileName());
        return out;
    }

    private SortedDataFile<K, V> getSortedFile(final String fileName) {
        final SortedDataFile<K, V> out = SortedDataFile.<K, V>builder()
                .withDirectory(directory).withFileName(fileName)
                .withKeyConvertorFromBytes(
                        keyTypeDescriptor.getConvertorFromBytes())
                .withValueReader(valueTypeDescriptor.getTypeReader())
                .withKeyConvertorToBytes(
                        keyTypeDescriptor.getConvertorToBytes())
                .withValueWriter(valueTypeDescriptor.getTypeWriter())
                .withKeyComparator(keyTypeDescriptor.getComparator()).build();
        return out;
    }

    /**
     * Writer ignores previously written data. Just append open data writer for
     * new data.
     * 
     * @return
     */
    public PairFileWriter<K, V> openCacheWriter() {
        final Access access = directory.isFileExists(getCacheFileName())
                ? Access.APPEND
                : Access.OVERWRITE;
        final PairFileWriter<K, V> basePairWriter = new UnsortedDataFileWriter<>(
                directory, getCacheFileName(),
                keyTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeWriter(), access);
        return new PairWriterCountPair<>(basePairWriter, props);
    }

    private String getCacheFileName() {
        return fileName + "-cache.unsorted";
    }

    private String getSortedFileName() {
        return fileName + ".sorted";
    }

    private String getMergedFileName() {
        return fileName + ".merged";
    }

}
