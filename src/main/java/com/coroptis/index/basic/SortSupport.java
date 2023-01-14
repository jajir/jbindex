package com.coroptis.index.basic;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.DataFileIterator;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;

public class SortSupport<K, V> {

    private final static String ROUND_NAME = "round";
    private final static String ROUND_SEPARTOR = "-";

    private final Directory directory;
    private final BasicIndex<K, V> basicIndex;
    private final ValueMerger<K, V> merger;

    SortSupport(final BasicIndex<K, V> basicIndex, final ValueMerger<K, V> merger) {
	this.basicIndex = Objects.requireNonNull(basicIndex);
	this.merger = Objects.requireNonNull(merger);
	this.directory = basicIndex.getDirectory();
    }

    List<String> getFilesInRound(final int roundNo) {
	return directory.getFileNames().filter(fileName -> isFileInRound(roundNo, fileName))
		.collect(Collectors.toList());
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

    String makeFileName(final int roundNo, final int no) {
	return ROUND_NAME + ROUND_SEPARTOR + roundNo + ROUND_SEPARTOR + no;
    }

    void mergeSortedFiles(final List<String> filesToMergeLocaly, final String producedFile) {
	final SortedDataFile<K, V> sortedFile = basicIndex.getSortedDataFile(producedFile);
	try (final SortedDataFileWriter<K, V> indexWriter = sortedFile.openWriter()) {
	    mergeSortedFiles(filesToMergeLocaly, pair -> indexWriter.put(pair));
	}
    }

    void mergeSortedFiles(final List<String> filesToMergeLocaly, final Consumer<Pair<K, V>> consumer) {
	List<DataFileIterator<K, V>> readers = null;
	try {
	    readers = filesToMergeLocaly.stream().map(fileName -> basicIndex.getSortedDataFile(fileName).openIterator())
		    .collect(Collectors.toList());
	    final MergeSpliterator<K, V> mergeSpliterator = new MergeSpliterator<K, V>(readers,
		    basicIndex.getKeyComparator(), merger);
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

}
