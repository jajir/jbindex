package com.coroptis.index.fileindex;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.PairComparator;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

public class FileIndexReader<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final PairComparator<K, V> pairComparator;
    private final PairReader<K, V> pairReader;

    public FileIndexReader(final FileReader fileReader,
	    final ConvertorFromBytes<K> keyConvertorToBytes,
	    final TypeReader<V> valueReader, final Comparator<? super K> keyComparator) {
	this.fileReader = Objects.requireNonNull(fileReader);
	final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(keyConvertorToBytes);
	pairReader = new PairReader<>(diffKeyReader, valueReader);
	pairComparator = new PairComparator<>(keyComparator);
    }

    public Pair<K, V> read() {
	return pairReader.read(fileReader);
    }

    public void skip(final int position) {
	fileReader.skip(position);
    }

    public Stream<Pair<K, V>> stream(final long estimateSize) {
	return StreamSupport.stream(
		new FileIndexSpliterator<>(pairReader, fileReader, pairComparator, estimateSize),
		false);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
