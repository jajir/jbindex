package com.coroptis.store;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeReader;

public class StoreStreamer<K, V> extends FileStreamer<K, V> implements CloseableResource {

    public StoreStreamer(final Directory directory, final TypeReader<K> keyReader, final TypeReader<V> valueReader) {
	super(directory, StoreWriter.STORE, keyReader, valueReader);
    }

    public StoreStreamer(final Directory directory, final Class<?> keyClass, final Class<?> valueClass) {
	super(directory, StoreWriter.STORE, TypeConvertors.getInstance().get(keyClass, OperationType.READER),
		TypeConvertors.getInstance().get(valueClass, OperationType.READER));
    }

}
