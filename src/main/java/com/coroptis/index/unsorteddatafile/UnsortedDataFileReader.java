package com.coroptis.index.unsorteddatafile;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileReader<K, V> implements CloseableResource {

    private final PairReader<K, V> pairReader;

    private final FileReader fileReader;

    private Pair<K, V> currentPair;

    public UnsortedDataFileReader(final Directory directory, final String fileName, final TypeReader<K> keyReader,
	    final TypeReader<V> valueReader) {
	Objects.requireNonNull(directory);
	Objects.requireNonNull(fileName);
	Objects.requireNonNull(keyReader);
	Objects.requireNonNull(valueReader);
	this.fileReader = directory.getFileReader(fileName);
	this.pairReader = new PairReader<K, V>(keyReader, valueReader);
	tryToReadNextPair();
    }

    public Optional<Pair<K, V>> readCurrent() {
	return Optional.ofNullable(currentPair);
    }

    public void moveToNext() {
	tryToReadNextPair();
    }

    private void tryToReadNextPair() {
	currentPair = pairReader.read(fileReader);
    }

    @Override
    public void close() {
	fileReader.close();

    }

}
