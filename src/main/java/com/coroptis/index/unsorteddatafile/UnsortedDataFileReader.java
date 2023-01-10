package com.coroptis.index.unsorteddatafile;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileReader<K, V> implements CloseableResource {

    private final PairReader<K, V> pairReader;

    private final FileReader fileReader;

    private Pair<K, V> currentPair;

    public UnsortedDataFileReader(final Directory directory, final Class<?> keyClass, final Class<?> valueClass) {
	final TypeConvertors tc = TypeConvertors.getInstance();
	this.fileReader = directory.getFileReader(UnsortedDataFileWriter.STORE);
	final TypeReader<K> keyReader = tc.get(Objects.requireNonNull(keyClass), OperationType.READER);
	final TypeReader<V> valueReader = tc.get(Objects.requireNonNull(valueClass), OperationType.READER);
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
