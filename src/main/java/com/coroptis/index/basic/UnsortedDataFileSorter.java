package com.coroptis.index.basic;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.coroptis.index.DataFileIterator;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

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

    private final static int HOW_MANY_FILES_TO_MERGE_AT_ONCE = 50;

    /**
     * Unsorted data file name.
     */
    private final String fileName;
    private final ValueMerger<K, V> merger;
    private final Comparator<? super K> keyComparator;
    private final Integer howManySortInMemory;
    private final BasicIndex<K, V> basicIndex;
    private final SortSupport<K, V> sortSupport;
    private final RoundSorted<K, V> roundSorter;

    UnsortedDataFileSorter(final String unsortedFileName, final ValueMerger<K, V> merger,
	    final Comparator<? super K> keyComparator, final Integer howManySortInMemory,
	    final BasicIndex<K, V> basicIndex) {
	this.fileName = Objects.requireNonNull(unsortedFileName);
	this.merger = Objects.requireNonNull(merger);
	this.howManySortInMemory = Objects.requireNonNull(howManySortInMemory);
	this.basicIndex = Objects.requireNonNull(basicIndex);
	this.keyComparator = Objects.requireNonNull(keyComparator);
	this.sortSupport = new SortSupport<>(basicIndex, merger);
	this.roundSorter = new RoundSorted<>(basicIndex, sortSupport, HOW_MANY_FILES_TO_MERGE_AT_ONCE);
    }

    public void consumeSortedData(final Consumer<Pair<K, V>> consumer) {
	Objects.requireNonNull(consumer);
	splitIntoSortedIndexes();
	int roundNo = 0;
	while (roundSorter.mergeRound(roundNo, consumer)) {
	    roundNo++;
	}
    }

    private void splitIntoSortedIndexes() {
	final UnsortedDataFile<K, V> unsortedFile = basicIndex.getUnsortedFile(fileName);
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
	basicIndex.deleteFile(fileName);
    }

    private void writeSortedListToFile(final List<Pair<K, V>> cache, final int fileCounter) {
	Collections.sort(cache, (pair1, pair2) -> keyComparator.compare(pair1.getKey(), pair2.getKey()));
	final String fileName = sortSupport.makeFileName(0, fileCounter);
	final SortedDataFile<K, V> sortedFile = basicIndex.getSortedDataFile(fileName);
	try (final SortedDataFileWriter<K, V> mainIndex = sortedFile.openWriter()) {
	    cache.forEach(pair -> mainIndex.put(pair));
	}
    }

}
