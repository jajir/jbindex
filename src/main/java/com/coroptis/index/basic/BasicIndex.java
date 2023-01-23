package com.coroptis.index.basic;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.coroptis.index.IndexException;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.partiallysorteddatafile.PartiallySortedDataFile;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

/**
 * Allows to create data files in directory and support further work with them.
 * It support work with individual files.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class BasicIndex<K, V> {

    private final Directory directory;
    private final Class<?> keyClass;
    private final Class<?> valueClass;
    private final ValueMerger<K, V> valueMerger;

    public BasicIndex(final Directory directory, final Class<?> keyClass, final Class<?> valueClass) {
	this(directory, keyClass, valueClass, new DefaultValueMerger<>());
    }

    public BasicIndex(final Directory directory, final Class<?> keyClass, final Class<?> valueClass,
	    final ValueMerger<K, V> valueMerger) {
	this.directory = Objects.requireNonNull(directory);
	this.keyClass = Objects.requireNonNull(keyClass);
	this.valueClass = Objects.requireNonNull(valueClass);
	this.valueMerger = Objects.requireNonNull(valueMerger);
    }

    public Directory getDirectory() {
	return directory;
    }

    Class<?> getKeyClass() {
	return keyClass;
    }

    Class<?> getValueClass() {
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

    public PartiallySortedDataFile<K, V> getPartiallySortedDataFile(final String fileName) {
	final PartiallySortedDataFile<K, V> out = PartiallySortedDataFile.<K, V>builder().withDirectory(getDirectory())
		.withFileName(fileName).withKeyComparator(getKeyComparator()).withBasicIndex(this)
		.withValueMerger(valueMerger).build();
	return out;
    }

    /**
     * When it's called sorting of given file starts. For each record in correct
     * order will be called given consumer. When method call returns than there are
     * no more records to sort.
     * 
     * @param unsortedFileName required unsorted file name
     * @param consumer         required consumer. All sorted data will be passed to
     *                         consumer.
     */
    public void consumeSortedData(final String unsortedFileName, final Consumer<Pair<K, V>> consumer,
	    final Integer howManySortInMemory) {
	final UnsortedDataFileSorter<K, V> sorter = new UnsortedDataFileSorter<>(unsortedFileName, valueMerger,
		getKeyComparator(), howManySortInMemory, this);
	sorter.consumeSortedData(consumer);
    }

    public void consumeSortedDataFromPartialySortedDataFile(final String partialySortedFileName,
	    final Consumer<Pair<K, V>> consumer, final Integer howManySortInMemory) {
	final UnsortedDataFileSorter<K, V> sorter = new UnsortedDataFileSorter<>(partialySortedFileName, valueMerger,
		getKeyComparator(), howManySortInMemory, this);
	sorter.consumePreSortedData(consumer);
    }

    public boolean deleteFile(final String fileName) {
	if (!directory.deleteFile(fileName)) {
	    throw new IndexException(
		    String.format("Unable to delte file '%s' in directory %s", fileName, directory.toString()));
	}
	return directory.deleteFile(fileName);
    }

    public Stream<String> getFileNames() {
	return directory.getFileNames();
    }

}
