package com.coroptis.store;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.TypeReader;

public class FileStreamer<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final TypeReader<K> keyReader;
    private final TypeReader<V> valueReader;

    public FileStreamer(final Directory directory, final String fileName,
	    final TypeReader<K> keyReader, final TypeReader<V> valueReader) {
	this.fileReader = directory.getFileReader(fileName);
	this.keyReader = Objects.requireNonNull(keyReader);
	this.valueReader = Objects.requireNonNull(valueReader);
    }

    public Stream<Pair<K, V>> stream() {
	return StreamSupport.stream(
		new StoreSpliterator<>(fileReader, new PairReader<K, V>(keyReader, valueReader)),
		false);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
