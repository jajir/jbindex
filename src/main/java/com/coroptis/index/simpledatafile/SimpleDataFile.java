package com.coroptis.index.simpledatafile;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.PairWriter;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.TypeDescriptor;

public class SimpleDataFile<K, V> implements CloseableResource {

    public SimpleDataFile(final Directory directory, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final ValueMerger<K, V> valueMerger) {

    }

    public PairWriter<K, V> openPairWriter() {
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
