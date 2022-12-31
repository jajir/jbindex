package com.coroptis.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.IndexWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.sorteddatafile.SortedDataFileReader;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;

/**
 * 1. merge two indexes into one.
 * 
 * 
 * 2. split unsorted index into n.
 * 
 * 
 * @param <K>
 * @param <V>
 */
public class StoreSorter<K, V> {

    private final static String ROUND_NAME = "round";
    private final static String ROUND_SEPARTOR = "-";
    private final static int HOW_MANY_FILES_TO_MERGE_AT_ONCE = 50;

    private final Random random = new Random();
    private final Directory directory;
    private final Merger<K, V> merger;
    private final Class<?> keyClass;
    private final Class<?> valueClass;
    private final TypeReader<K> keyReader;
    private final TypeReader<V> valueReader;
    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final Comparator<? super K> keyComparator;
    private final Integer howManySortInMemory;
    private final Integer blockSize;

    public StoreSorter(final Directory directory, final Merger<K, V> merger, final Class<?> keyClass,
	    final Class<?> valueClass, final Integer howManySortInMemory, final Integer blockSize) {
	this.directory = Objects.requireNonNull(directory);
	this.merger = Objects.requireNonNull(merger);
	this.keyClass = Objects.requireNonNull(keyClass);
	this.valueClass = Objects.requireNonNull(valueClass);
	this.howManySortInMemory = Objects.requireNonNull(howManySortInMemory);
	this.blockSize = Objects.requireNonNull(blockSize);
	final TypeConvertors tc = TypeConvertors.getInstance();
	keyReader = tc.get(keyClass, OperationType.READER);
	valueReader = tc.get(valueClass, OperationType.READER);
	keyWriter = tc.get(keyClass, OperationType.WRITER);
	valueWriter = tc.get(valueClass, OperationType.WRITER);
	keyComparator = tc.get(keyClass, OperationType.COMPARATOR);
    }

    public void sort() {
	splitIntoSortedIndexes();
	int roundNo = 0;
	while (mergeRound2(roundNo) != 1) {
	    roundNo++;
	}
    }

    private void splitIntoSortedIndexes() {
	final PairReader<K, V> pairReader = new PairReader<K, V>(keyReader, valueReader);
	try (final FileReader fileReader = directory.getFileReader(StoreWriter.STORE)) {
	    final StoreReader<K, V> reader = new StoreReader<K, V>(pairReader, fileReader);
	    int cx = 0;
	    int fileCounter = 0;
	    final UniqueCache<K, V> cache = new UniqueCache<>(merger);
	    while (reader.readCurrent().isPresent()) {
		cache.add(reader.readCurrent().get());
		reader.moveToNext();
		cx++;
		if (cx % howManySortInMemory == 0) {
		    writeSortedListToFile(cache.toList(), fileCounter);
		    cache.clear();
		    fileCounter++;
		}
	    }
	    writeSortedListToFile(cache.toList(), fileCounter);
	}
	directory.deleteFile(StoreWriter.STORE);
    }

    private void writeSortedListToFile(final List<Pair<K, V>> cache, final int fileCounter) {
	Collections.sort(cache, (pair1, pair2) -> keyComparator.compare(pair1.getKey(), pair2.getKey()));
	final String fileName = makeFileName(0, fileCounter);

	final TypeConvertors tc = TypeConvertors.getInstance();
	final ConvertorToBytes<K> keyConvertor = tc.get(keyClass, OperationType.CONVERTOR_TO_BYTES);
	try (final SortedDataFileWriter<K, V> mainIndex = new SortedDataFileWriter<>(directory.getFileWriter(fileName),
		keyConvertor, keyComparator, valueWriter)) {
	    cache.forEach(pair -> mainIndex.put(pair));
	}
    }

    private int mergeRound2(final int roundNo) {
	final List<String> filesToMerge = getFilesInRound(roundNo);

	if (filesToMerge.size() == 0) {
	    try (final IndexWriter<K, V> indexWriter = new IndexWriter<K, V>(directory, 3, keyClass, valueClass)) {
		// do nothing, just create empty index.
	    }
	    return 1;
	}

	if (filesToMerge.size() == 1) {
	    final TypeConvertors tc = TypeConvertors.getInstance();
	    final ConvertorFromBytes<K> keyConvertor = tc.get(keyClass, OperationType.CONVERTOR_FROM_BYTES);
	    try (final SortedDataFileReader<K, V> fileStreamer = new SortedDataFileReader<K, V>(
		    directory.getFileReader(filesToMerge.get(0)), keyConvertor, valueReader, keyComparator)) {
		// create final index
		try (final IndexWriter<K, V> indexWriter = new IndexWriter<K, V>(directory, blockSize, keyClass,
			valueClass)) {
		    fileStreamer.stream(1).forEach(pair -> indexWriter.put(pair.getKey(), pair.getValue()));
		}
	    }
	    directory.deleteFile(filesToMerge.get(0));
	    return 1;
	}

	for (int i = 0; i < howManyFilesShoulBeProduces(filesToMerge.size()); i++) {
	    final List<String> filesToMergeLocaly = new ArrayList<>();
	    for (int j = 0; j < HOW_MANY_FILES_TO_MERGE_AT_ONCE; j++) {
		final int index = i + j;
		if (index < filesToMerge.size()) {
		    final String fileName = filesToMerge.get(index);
		    filesToMergeLocaly.add(fileName);
		}
	    }
	    final TypeConvertors tc = TypeConvertors.getInstance();
	    final ConvertorToBytes<K> keyConvertor = tc.get(keyClass, OperationType.CONVERTOR_TO_BYTES);
	    try (final SortedDataFileWriter<K, V> indexWriter = new SortedDataFileWriter<>(
		    directory.getFileWriter(makeFileName(roundNo + 1, i)), keyConvertor, keyComparator, valueWriter)) {
		mergeSortedFiles(filesToMergeLocaly, pair -> indexWriter.put(pair));
	    }
	    filesToMergeLocaly.forEach(fileName -> directory.deleteFile(fileName));
	    // 1. pripravit seznam soubor u
	    // 2. vsechny otevrit a uzavrit
	    // 3. poslat do mergeru ;-)
	    // 4. ppuvodni soubory smazat
	}
	return 0;
    }

    /**
     * Divide number of which should be merged by how many files should be merged at
     * on run. Result is rounded up.
     * 
     * @param numberOfFiles return number of round which should merge files
     * @return
     */
    private int howManyFilesShoulBeProduces(final int numberOfFiles) {
	return (int) Math.ceil(((float) numberOfFiles) / ((float) HOW_MANY_FILES_TO_MERGE_AT_ONCE));
    }

    private List<String> getFilesInRound(final int roundNo) {
	return directory.getFileNames().filter(fileName -> isFileInRound(roundNo, fileName))
		.collect(Collectors.toList());
    };

    private void mergeSortedFiles(final List<String> filesToMergeLocaly, final Consumer<Pair<K, V>> consumer) {
	final TypeConvertors tc = TypeConvertors.getInstance();
	final ConvertorFromBytes<K> keyConvertor = tc.get(keyClass, OperationType.CONVERTOR_FROM_BYTES);
	List<SortedDataFileReader<K, V>> readers = null;
	try {
	    readers = filesToMergeLocaly.stream()
		    .map(fileName -> new SortedDataFileReader<K, V>(directory.getFileReader(fileName), keyConvertor,
			    valueReader, keyComparator))
		    .collect(Collectors.toList());
	    final MergeSpliterator<K, V> mergeSpliterator = new MergeSpliterator<K, V>(readers, keyComparator, merger);
	    final Stream<Pair<K, V>> pairStream = StreamSupport.stream(mergeSpliterator, false);
	    pairStream.forEach(pair -> consumer.accept(pair));
	} finally {
	    if (readers != null) {
		readers.forEach(reader -> {
		    try {
			reader.close();
		    } catch (Exception e) {
			// Just closing all readers, when exceptions occurs I don't care.
		    }
		});
	    }
	}
    }

    private String makeFileName(final int roundNo, final int no) {
	return ROUND_NAME + ROUND_SEPARTOR + roundNo + ROUND_SEPARTOR + no;
    }

    private boolean isFileInRound(final int roundNo, final String fileName) {
	final String[] parts = fileName.split(ROUND_SEPARTOR);
	if (parts.length == 3) {
	    int currentRoundNo = Integer.valueOf(parts[1]);
	    return roundNo == currentRoundNo;
	} else {
	    return false;
	}
    }

}
