package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.PairComparator;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

public class SortedDataFileStreamer<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final PairComparator<K, V> pairComparator;
    private final PairReader<K, V> pairReader;

    public SortedDataFileStreamer(final Directory directory, final String fileName,
	    final ConvertorFromBytes<K> keyConvertorFromBytes, final TypeReader<V> valueReader,
	    final Comparator<? super K> keyComparator) {
	Objects.requireNonNull(directory);
	Objects.requireNonNull(fileName);
	this.fileReader = directory.getFileReader(fileName);
	final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(keyConvertorFromBytes);
	pairReader = new PairReader<>(diffKeyReader, valueReader);
	pairComparator = new PairComparator<>(keyComparator);
    }

    /**
     * Allows to initialize object and skip some initial bytes.
     * 
     * @param directory
     * @param fileName
     * @param keyConvertorFromBytes
     * @param valueReader
     * @param keyComparator
     * @param streamFromPosition
     */
    public SortedDataFileStreamer(final Directory directory, final String fileName,
	    final ConvertorFromBytes<K> keyConvertorFromBytes, final TypeReader<V> valueReader,
	    final Comparator<? super K> keyComparator, final long streamFromPosition) {
	this(directory, fileName, keyConvertorFromBytes, valueReader, keyComparator);
	fileReader.skip(streamFromPosition);
    }

    public Stream<Pair<K, V>> stream() {
	return StreamSupport.stream(new SortedDataFileSpliterator<>(pairReader, fileReader, pairComparator), false);
    }

    public Stream<Pair<K, V>> stream(final long estimateSize) {
	return StreamSupport.stream(
		new SortedDataFileSpliteratorSized<>(pairReader, fileReader, pairComparator, estimateSize), false);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
