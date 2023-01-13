package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.DataFileIterator;
import com.coroptis.index.DataFileReader;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;

public class SortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<V> valueReader;

    private final Comparator<? super K> keyComparator;

    private final ConvertorFromBytes<K> keyConvertorFromBytes;

    private final ConvertorToBytes<K> keyConvertorToBytes;

    public static <M, N> SortedDataFileBuilder<M, N> builder() {
	return new SortedDataFileBuilder<M, N>();
    }

    public SortedDataFile(final Directory directory, final String fileName, final TypeWriter<V> valueWriter,
	    final TypeReader<V> valueReader, final Comparator<? super K> keyComparator,
	    final ConvertorFromBytes<K> keyConvertorFromBytes, final ConvertorToBytes<K> keyConvertorToBytes) {
	this.directory = Objects.requireNonNull(directory);
	this.fileName = Objects.requireNonNull(fileName);
	this.valueWriter = Objects.requireNonNull(valueWriter);
	this.valueReader = Objects.requireNonNull(valueReader);
	this.keyComparator = Objects.requireNonNull(keyComparator);
	this.keyConvertorFromBytes = Objects.requireNonNull(keyConvertorFromBytes);
	this.keyConvertorToBytes = Objects.requireNonNull(keyConvertorToBytes);
    }

    public SortedDataFileStreamer<K, V> openStreamer() {
	final SortedDataFileStreamer<K, V> streamer = new SortedDataFileStreamer<>(directory, fileName,
		keyConvertorFromBytes, valueReader, keyComparator);
	return streamer;
    }

    public SortedDataFileStreamer<K, V> openStreamer(final long skipInitialBytes) {
	final SortedDataFileStreamer<K, V> streamer = new SortedDataFileStreamer<>(directory, fileName,
		keyConvertorFromBytes, valueReader, keyComparator, skipInitialBytes);
	return streamer;
    }

    public DataFileReader<K, V> openReader() {
	final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(keyConvertorFromBytes);
	final PairReader<K, V> pairReader = new PairReader<>(diffKeyReader, valueReader);
	final DataFileReader<K, V> reader = new DataFileReader<>(directory, fileName, pairReader);
	return reader;
    }

    public DataFileIterator<K, V> openIterator() {
	final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(keyConvertorFromBytes);
	final PairReader<K, V> pairReader = new PairReader<>(diffKeyReader, valueReader);
	final DataFileIterator<K, V> iterator = new DataFileIterator<>(directory, fileName, pairReader);
	return iterator;
    }

    public SortedDataFileWriter<K, V> openWriter() {
	final SortedDataFileWriter<K, V> writer = new SortedDataFileWriter<>(directory, fileName, keyConvertorToBytes,
		keyComparator, valueWriter);
	return writer;
    }

}
