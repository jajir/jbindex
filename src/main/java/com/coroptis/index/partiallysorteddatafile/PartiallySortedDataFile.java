package com.coroptis.index.partiallysorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.SortSupport;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;

public class PartiallySortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final Comparator<? super K> keyComparator;

    private final BasicIndex<K, V> basicIndex;

    private final ValueMerger<K, V> merger;

    public static <M, N> PartiallySortedDataFileBuilder<M, N> builder() {
	return new PartiallySortedDataFileBuilder<M, N>();
    }

    public PartiallySortedDataFile(final Directory directory, final String fileName,
	    final Comparator<? super K> keyComparator, final BasicIndex<K, V> basicIndex,
	    final ValueMerger<K, V> merger) {
	this.directory = Objects.requireNonNull(directory);
	this.fileName = Objects.requireNonNull(fileName);
	this.keyComparator = Objects.requireNonNull(keyComparator);
	this.basicIndex = Objects.requireNonNull(basicIndex);
	this.merger = Objects.requireNonNull(merger);
    }

    // TODO 1. create interface for reader
    // TODO 2. return interface instead of implementation
    // TODO 3. use reader interface for streamer and iterator
    public PartiallySortedDataFileReader<K, V> openReader() {
	final SortSupport<K, V> sortSupport = new SortSupport<>(basicIndex, merger, fileName);
	final PartiallySortedDataFileReader<K, V> reader = new PartiallySortedDataFileReader<>(basicIndex, sortSupport);
	return reader;
    }

    public PartiallySortedDataFileWriter<K, V> openWriter(int howManySortInMemory) {
	final PartiallySortedDataFileWriter<K, V> writer = new PartiallySortedDataFileWriter<>(fileName, merger,
		howManySortInMemory, basicIndex, keyComparator);
	return writer;
    }

}
