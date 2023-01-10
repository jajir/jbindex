package com.coroptis.index.unsorteddatafile;

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.PairReader;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeReader;

public class UnsortedDataFileStreamer<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final TypeReader<K> keyReader;
    private final TypeReader<V> valueReader;

    public UnsortedDataFileStreamer(final Directory directory, final Class<?> keyClass, final Class<?> valueClass) {
	final TypeConvertors tc = TypeConvertors.getInstance();
	this.fileReader = directory.getFileReader(UnsortedDataFileWriter.STORE);
	this.keyReader = tc.get(Objects.requireNonNull(keyClass), OperationType.READER);
	this.valueReader = tc.get(Objects.requireNonNull(valueClass), OperationType.READER);

    }

    public Stream<Pair<K, V>> stream() {
	return StreamSupport.stream(new UnsortedDataFileSpliterator<>(fileReader, new PairReader<K, V>(keyReader, valueReader)),
		false);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
