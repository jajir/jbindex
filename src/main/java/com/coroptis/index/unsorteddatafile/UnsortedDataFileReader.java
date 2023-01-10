package com.coroptis.index.unsorteddatafile;

import java.util.Optional;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.IndexConfiguration;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileReader<K, V> implements CloseableResource {

    private final PairReader<K, V> pairReader;

    private final FileReader fileReader;

    private Pair<K, V> currentPair;

    //FIXME jmeno souboru ma prijit jako parameter
    public UnsortedDataFileReader(final IndexConfiguration<K, V> indexConfiguration) {
	this.fileReader = indexConfiguration.getDirectory().getFileReader(UnsortedDataFileWriter.STORE);
	final TypeReader<K> keyReader = indexConfiguration.getKeyReader();
	final TypeReader<V> valueReader = indexConfiguration.getValueReader();
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
