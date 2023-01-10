package com.coroptis.index.unsorteddatafile;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.IndexConfiguration;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileStreamer<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final TypeReader<K> keyReader;
    private final TypeReader<V> valueReader;

    public UnsortedDataFileStreamer(final IndexConfiguration<K, V> indexConfiguration) {
	this.fileReader = indexConfiguration.getDirectory().getFileReader(UnsortedDataFileWriter.STORE);
	this.keyReader = indexConfiguration.getKeyReader();
	this.valueReader = indexConfiguration.getValueReader();

    }

    public Stream<Pair<K, V>> stream() {
	return StreamSupport.stream(
		new UnsortedDataFileSpliterator<>(fileReader, new PairReader<K, V>(keyReader, valueReader)), false);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
