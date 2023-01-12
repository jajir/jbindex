package com.coroptis.index.unsorteddatafile;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.TypeReader;
import com.coroptis.index.type.TypeWriter;

public class UnsortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final TypeWriter<K> keyWriter;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<K> keyReader;

    private final TypeReader<V> valueReader;

    public static <M, N> UnsortedDataFileBuilder<M, N> builder() {
	return new UnsortedDataFileBuilder<M, N>();
    }

    public UnsortedDataFile(final Directory directory, final String fileName, final TypeWriter<K> keyWriter,
	    final TypeWriter<V> valueWriter, final TypeReader<K> keyReader, final TypeReader<V> valueReader) {
	this.directory = Objects.requireNonNull(directory);
	this.fileName = Objects.requireNonNull(fileName);
	this.keyWriter = Objects.requireNonNull(keyWriter);
	this.valueWriter = Objects.requireNonNull(valueWriter);
	this.keyReader = Objects.requireNonNull(keyReader);
	this.valueReader = Objects.requireNonNull(valueReader);
    }

    public UnsortedDataFileReader<K, V> openReader() {
	final UnsortedDataFileReader<K, V> reader = new UnsortedDataFileReader<>(directory, fileName, keyReader,
		valueReader);
	return reader;
    }

    public UnsortedDataFileWriter<K, V> openWriter() {
	final UnsortedDataFileWriter<K, V> writer = new UnsortedDataFileWriter<>(directory, fileName, keyWriter,
		valueWriter);
	return writer;
    }

    public UnsortedDataFileStreamer<K, V> openStreamer() {
	final UnsortedDataFileStreamer<K, V> streamer = new UnsortedDataFileStreamer<>(directory, fileName, keyReader,
		valueReader);
	return streamer;
    }

}
