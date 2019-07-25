package com.coroptis.store;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.ConvertorType;
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

    private final TypeReader<K> keyReader;
    private final TypeReader<V> valueReader;
    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;

    StoreSorter(final Directory directory, final Merger<K, V> merger, final Class<?> keyClass,
	    final Class<?> valueClass) {
	this.directory = Objects.requireNonNull(directory);
	this.merger = Objects.requireNonNull(merger);
	final TypeConvertors tc = TypeConvertors.getInstance();
	keyReader = tc.get(keyClass, ConvertorType.READER);
	valueReader = tc.get(valueClass, ConvertorType.READER);
	keyWriter = tc.get(keyClass, ConvertorType.WRITER);
	valueWriter = tc.get(valueClass, ConvertorType.WRITER);
    }

    private void splitIntoSortedIndexes() {

    }

    private boolean mergeRound(final int roundNo) {
	final List<String> filesToMerge = directory.getFileNames()
		.filter(fileName -> isFileInRound(roundNo, fileName)).collect(Collectors.toList());
	int currentFileNo = 0;
	if (filesToMerge.size() % 2 == 1) {
	    final int fileNo = random.nextInt(filesToMerge.size());
	    final String fileToMove = makeFileName(roundNo, fileNo);
	    renameFile(fileToMove, makeFileName(roundNo + 1, currentFileNo));
	    filesToMerge.remove(fileToMove);
	    currentFileNo++;
	}

	// Merge files. Number of files to merge is even number

	return true;
    }

    private boolean isFileInRound(final int roundNo, final String fileName) {
	final String[] parts = fileName.split(ROUND_SEPARTOR);
	int currentRoundNo = Integer.valueOf(parts[1]);
	return roundNo == currentRoundNo;
    }

    private void mergeSortedFiles(final String fileName1, final String fileName2,
	    final String mergedFileName) {

	try (final StoreFileStreamer<K, V> stream1 = new StoreFileStreamer<>(directory, fileName1,
		keyReader, valueReader)) {
	    try (final StoreFileStreamer<K, V> stream2 = new StoreFileStreamer<>(directory,
		    fileName2, keyReader, valueReader)) {
		try (final StoreFileWriter<K, V> output = new StoreFileWriter<>(directory,
			mergedFileName, keyWriter, valueWriter)) {
		}
	    }
	}
    }

    private String makeFileName(final int roundNo, final int no) {
	return ROUND_NAME + ROUND_SEPARTOR + roundNo + ROUND_SEPARTOR + no;
    }

    private void renameFile(final String currentFileName, final String newFileName) {
	// FIXME
    }

}
