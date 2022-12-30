package com.coroptis.store;

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
	while (mergeRound(roundNo) != 1) {
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

//	try (final StoreFileWriter<K, V> output = new StoreFileWriter<>(directory, fileName, keyWriter, valueWriter)) {
//	    cache.forEach(pair -> output.put(pair.getKey(), pair.getValue()));
//	}
    }

    // Return number of produced files.
    private int mergeRound(final int roundNo) {
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

	if (filesToMerge.size() == 2) {
	    final String fn1 = filesToMerge.get(0);
	    final String fn2 = filesToMerge.get(1);

	    try (final IndexWriter<K, V> indexWriter = new IndexWriter<K, V>(directory, blockSize, keyClass,
		    valueClass)) {
		mergeSortedFiles(fn1, fn2, pair -> indexWriter.put(pair.getKey(), pair.getValue()));
	    }

	    directory.deleteFile(fn1);
	    directory.deleteFile(fn2);
	    return 1;
	}

	int currentFileNo = 0;
	if (filesToMerge.size() % 2 == 1) {
	    final int fileNo = random.nextInt(filesToMerge.size());
	    final String fileToMove = makeFileName(roundNo, fileNo);
	    directory.renameFile(fileToMove, makeFileName(roundNo + 1, currentFileNo));
	    filesToMerge.remove(fileToMove);
	    currentFileNo++;
	}

	// Merge files. Number of files to merge is even number
	for (int i = 0; i < filesToMerge.size() / 2; i++) {
	    final String fn1 = filesToMerge.get(i * 2);
	    final String fn2 = filesToMerge.get(i * 2 + 1);

	    final TypeConvertors tc = TypeConvertors.getInstance();
	    final ConvertorToBytes<K> keyConvertor = tc.get(keyClass, OperationType.CONVERTOR_TO_BYTES);
	    try (final SortedDataFileWriter<K, V> indexWriter = new SortedDataFileWriter<>(
		    directory.getFileWriter(makeFileName(roundNo + 1, currentFileNo)), keyConvertor, keyComparator,
		    valueWriter)) {
//	    try (final StoreFileWriter<K, V> output = new StoreFileWriter<>(directory,
//		    makeFileName(roundNo + 1, currentFileNo), keyWriter, valueWriter)) {
		mergeSortedFiles(fn1, fn2, pair -> indexWriter.put(pair));
	    }

	    directory.deleteFile(fn1);
	    directory.deleteFile(fn2);
	    currentFileNo++;
	}

	return currentFileNo;
    }

    private List<String> getFilesInRound(final int roundNo) {
	return directory.getFileNames().filter(fileName -> isFileInRound(roundNo, fileName))
		.collect(Collectors.toList());
    };

    private void mergeSortedFiles(final String fileName1, final String fileName2, final Consumer<Pair<K, V>> consumer) {
	final PairReader<K, V> pairReader = new PairReader<K, V>(keyReader, valueReader);

	final TypeConvertors tc = TypeConvertors.getInstance();
	final ConvertorFromBytes<K> keyConvertor = tc.get(keyClass, OperationType.CONVERTOR_FROM_BYTES);
	try (final SortedDataFileReader<K, V> fileStreamer1 = new SortedDataFileReader<K, V>(
		directory.getFileReader(fileName1), keyConvertor, valueReader, keyComparator)) {
	    try (final SortedDataFileReader<K, V> fileStreamer2 = new SortedDataFileReader<K, V>(
		    directory.getFileReader(fileName2), keyConvertor, valueReader, keyComparator)) {
		final MergeSpliterator<K, V> mergeSpliterator = new MergeSpliterator<K, V>(fileStreamer1, fileStreamer2,
			keyComparator, merger);
		final Stream<Pair<K, V>> pairStream = StreamSupport.stream(mergeSpliterator, false);
		pairStream.forEach(pair -> consumer.accept(pair));
	    }
	}
//	try (final FileReader fileReader1 = directory.getFileReader(fileName1)) {
//	    final StoreReader<K, V> reader1 = new StoreReader<K, V>(pairReader, fileReader1);
//	    try (final FileReader fileReader2 = directory.getFileReader(fileName2)) {
//		final StoreReader<K, V> reader2 = new StoreReader<K, V>(pairReader, fileReader2);
//
//		final MergeSpliterator<K, V> mergeSpliterator = new MergeSpliterator<K, V>(reader1, reader2,
//			keyComparator, merger);
//
//		final Stream<Pair<K, V>> pairStream = StreamSupport.stream(mergeSpliterator, false);
//		pairStream.forEach(pair -> consumer.accept(pair));
//
//	    }
//	}
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
