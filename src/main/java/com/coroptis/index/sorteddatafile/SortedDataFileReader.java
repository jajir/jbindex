package com.coroptis.index.sorteddatafile;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileReader;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.TypeReader;

public class SortedDataFileReader<K, V> implements CloseableResource {

    private final FileReader fileReader;
    private final PairReader<K, V> pairReader;

    public SortedDataFileReader(final Directory directory, final String fileName,
	    final ConvertorFromBytes<K> keyConvertorToBytes, final TypeReader<V> valueReader) {
	Objects.requireNonNull(directory);
	Objects.requireNonNull(fileName);
	this.fileReader = directory.getFileReader(fileName);
	final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(keyConvertorToBytes);
	pairReader = new PairReader<>(diffKeyReader, valueReader);
    }

    /**
     * Try to read data.
     * 
     * @return Return read data when it's possible. Return <code>null</code> when
     *         there are no data.
     */
    public Pair<K, V> read() {
	return pairReader.read(fileReader);
    }

    public void skip(final int position) {
	fileReader.skip(position);
    }

    @Override
    public void close() {
	fileReader.close();
    }

}
