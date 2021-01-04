package com.coroptis.store;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;

public class StoreWriter<K, V> extends StoreFileWriter<K, V> implements CloseableResource {

    public final static String STORE = "unsorted.un";

    public StoreWriter(final Directory directory, final Class<?> keyClass,
	    final Class<?> valueClass) {
	super(directory, STORE, TypeConvertors.getInstance().get(keyClass, OperationType.WRITER),
		TypeConvertors.getInstance().get(valueClass, OperationType.WRITER));
    }

}
