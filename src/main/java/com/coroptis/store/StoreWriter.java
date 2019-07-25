package com.coroptis.store;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.simpleindex.CloseableResource;
import com.coroptis.index.type.TypeWriter;

public class StoreWriter<K, V> extends StoreFileWriter<K, V> implements CloseableResource {

    public final static String STORE = "unsorted";

    public StoreWriter(final Directory directory, final TypeWriter<K> keyWriter,
	    final TypeWriter<V> valueWriter) {
	super(directory, STORE, keyWriter, valueWriter);
    }

}
