package com.coroptis.index.unsorteddatafile;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileStreamer<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final PairReader<K, V> pairReader;

    public UnsortedDataFileStreamer(final Directory directory, final String file, final TypeReader<K> keyReader,
	    final TypeReader<V> valueReader) {
	Objects.requireNonNull(directory);
	Objects.requireNonNull(file);
	Objects.requireNonNull(keyReader);
	Objects.requireNonNull(valueReader);
	this.fileReader = directory.getFileReader(UnsortedDataFileWriter.STORE);
	this.pairReader = new PairReader<K, V>(keyReader, valueReader);
    }

    public Stream<Pair<K, V>> stream() {
	return StreamSupport.stream(new UnsortedDataFileSpliterator<>(fileReader, pairReader), false);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
