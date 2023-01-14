package com.coroptis.index.basic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.DataFileIterator;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileWriter;

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
class UnsortedDataFileSorter<K, V> {

    private final static String ROUND_NAME = "round";
    private final static String ROUND_SEPARTOR = "-";
    private final static int HOW_MANY_FILES_TO_MERGE_AT_ONCE = 50;

    private final String unsortedFileName;
    private final ValueMerger<K, V> merger;
    private final Comparator<? super K> keyComparator;
    private final Integer howManySortInMemory;
    private final BasicIndex<K, V> basicIndex;

    UnsortedDataFileSorter(final String unsortedFileName, final ValueMerger<K, V> merger,
	    final Comparator<? super K> keyComparator, final Integer howManySortInMemory,
	    final BasicIndex<K, V> basicIndex) {
	this.unsortedFileName = Objects.requireNonNull(unsortedFileName);
	this.merger = Objects.requireNonNull(merger);
	this.howManySortInMemory = Objects.requireNonNull(howManySortInMemory);
	this.basicIndex = Objects.requireNonNull(basicIndex);
	this.keyComparator = Objects.requireNonNull(keyComparator);
    }

    public void consumeSortedData(final Consumer<Pair<K, V>> consumer) {
	Objects.requireNonNull(consumer);
	splitIntoSortedIndexes();
	int roundNo = 0;
	while (mergeRound(roundNo, consumer)) {
	    roundNo++;
	}
    }

    private void splitIntoSortedIndexes() {
	final UnsortedDataFile<K, V> unsortedFile = basicIndex.getUnsortedFile(unsortedFileName);
	try (final DataFileIterator<K, V> reader = unsortedFile.openIterator()) {
	    int cx = 0;
	    int fileCounter = 0;
	    final UniqueCache<K, V> cache = new UniqueCache<>(merger);
	    while (reader.hasNext()) {
		cache.add(reader.readCurrent().get());
		reader.next();
		cx++;
		if (cx % howManySortInMemory == 0) {
		    writeSortedListToFile(cache.toList(), fileCounter);
		    cache.clear();
		    fileCounter++;
		}
	    }
	    writeSortedListToFile(cache.toList(), fileCounter);
	}
	basicIndex.deleteFile(unsortedFileName);
    }

    private void writeSortedListToFile(final List<Pair<K, V>> cache, final int fileCounter) {
	Collections.sort(cache, (pair1, pair2) -> keyComparator.compare(pair1.getKey(), pair2.getKey()));
	final String fileName = makeFileName(0, fileCounter);
	final SortedDataFile<K, V> sortedFile = basicIndex.getSortedDataFile(fileName);
	try (final SortedDataFileWriter<K, V> mainIndex = sortedFile.openWriter()) {
	    cache.forEach(pair -> mainIndex.put(pair));
	}
    }

    /**
     * 
     * @param roundNo
     * @param consumer
     * @return return <code>true</code> when next merging round should be done
     *         otherwise return <code>false</code>
     */
    private boolean mergeRound(final int roundNo, final Consumer<Pair<K, V>> consumer) {
	final List<String> filesToMerge = getFilesInRound(roundNo);

	if (filesToMerge.size() == 0) {
	    // do nothing
	    return false;
	}

	final int howManyFilesShouldBeProduces = howManyFilesShouldBeProduces(filesToMerge.size());
	final boolean isFinalMergingRound = howManyFilesShouldBeProduces == 1;

	for (int i = 0; i < howManyFilesShouldBeProduces(filesToMerge.size()); i++) {
	    final List<String> filesToMergeLocaly = new ArrayList<>();
	    for (int j = 0; j < HOW_MANY_FILES_TO_MERGE_AT_ONCE; j++) {
		final int index = i + j;
		if (index < filesToMerge.size()) {
		    final String fileName = filesToMerge.get(index);
		    filesToMergeLocaly.add(fileName);
		}
	    }
	    if (isFinalMergingRound) {
		mergeSortedFiles(filesToMergeLocaly, consumer::accept);
	    } else {
		final SortedDataFile<K, V> sortedFile = basicIndex.getSortedDataFile(makeFileName(roundNo + 1, i));
		try (final SortedDataFileWriter<K, V> indexWriter = sortedFile.openWriter()) {
		    mergeSortedFiles(filesToMergeLocaly, pair -> indexWriter.put(pair));
		}
	    }
	    filesToMergeLocaly.forEach(fileName -> basicIndex.deleteFile(fileName));
	}

	return !isFinalMergingRound;
    }

    /**
     * Divide number of which should be merged by how many files should be merged at
     * on run. Result is rounded up.
     * 
     * @param numberOfFiles return number of round which should merge files
     * @return
     */
    private int howManyFilesShouldBeProduces(final int numberOfFiles) {
	return (int) Math.ceil(((float) numberOfFiles) / ((float) HOW_MANY_FILES_TO_MERGE_AT_ONCE));
    }

    private List<String> getFilesInRound(final int roundNo) {
	return basicIndex.getFileNames().filter(fileName -> isFileInRound(roundNo, fileName))
		.collect(Collectors.toList());
    }

    private void mergeSortedFiles(final List<String> filesToMergeLocaly, final Consumer<Pair<K, V>> consumer) {
	List<DataFileIterator<K, V>> readers = null;
	try {
	    readers = filesToMergeLocaly.stream().map(fileName -> basicIndex.getSortedDataFile(fileName).openIterator())
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
