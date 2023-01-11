package com.coroptis.index.unsorteddatafile;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.IndexConfiguration;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileReader<K, V> implements CloseableResource {

    private final PairReader<K, V> pairReader;

    private final FileReader fileReader;

    private Pair<K, V> currentPair;

    @Deprecated
    public UnsortedDataFileReader(final IndexConfiguration<K, V> indexConfiguration) {
	this(indexConfiguration.getDirectory(), indexConfiguration.getKeyReader(), indexConfiguration.getValueReader());
    }

    @Deprecated
    public UnsortedDataFileReader(final Directory directory, final TypeReader<K> keyReader,
	    final TypeReader<V> valueReader) {
	this(directory, UnsortedDataFileWriter.STORE, keyReader, valueReader);
    }

    public UnsortedDataFileReader(final Directory directory, final String file, final TypeReader<K> keyReader,
	    final TypeReader<V> valueReader) {
	Objects.requireNonNull(directory);
	Objects.requireNonNull(file);
	Objects.requireNonNull(keyReader);
	Objects.requireNonNull(valueReader);
	this.fileReader = directory.getFileReader(UnsortedDataFileWriter.STORE);
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
