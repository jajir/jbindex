package com.coroptis.store;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.simpleindex.CloseableResource;
import com.coroptis.index.type.TypeReader;

public class StoreReader<K, V> extends StoreFileStreamer<K, V> implements CloseableResource {

    public StoreReader(final Directory directory, final TypeReader<K> keyReader,
	    final TypeReader<V> valueReader) {
	super(directory, StoreWriter.STORE, keyReader, valueReader);
    }

}
