package com.coroptis.index.unsorteddatafile;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.PairTypeReader;
import com.coroptis.index.sorteddatafile.PairTypeReaderImpl;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileStreamer<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final PairTypeReader<K, V> pairReader;

    UnsortedDataFileStreamer(final Directory directory, final String fileName, final TypeReader<K> keyReader,
	    final TypeReader<V> valueReader) {
	Objects.requireNonNull(directory);
	Objects.requireNonNull(fileName);
	Objects.requireNonNull(keyReader);
	Objects.requireNonNull(valueReader);

	if (directory.isFileExists(fileName)) {
	    this.fileReader = directory.getFileReader(fileName);
	    this.pairReader = new PairTypeReaderImpl<K, V>(keyReader, valueReader);
	} else {
	    this.fileReader = null;
	    this.pairReader = null;
	}
    }

    public Stream<Pair<K, V>> stream() {
	if (fileReader == null) {
	    return Stream.empty();
	} else {
	    return StreamSupport.stream(new UnsortedDataFileSpliterator<>(fileReader, pairReader), false);
	}
    }

    @Override
    public void close() {
	if (fileReader != null) {
	    fileReader.close();
	}
    }

}
