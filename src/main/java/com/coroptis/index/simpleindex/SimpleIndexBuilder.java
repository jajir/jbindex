package com.coroptis.index.simpleindex;

import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.type.TypeDescriptor;

public class SimpleIndexBuilder<K, V> {

    private Directory directory;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private ValueMerger<K, V> valueMerger;

    public SimpleIndex<K, V> buid() {
        return new SimpleIndex<>(directory, valueMerger, keyTypeDescriptor, valueTypeDescriptor);
    }

    public SimpleIndexBuilder<K, V> directory(final Directory directory) {
        this.directory = directory;
        return this;
    }

    public SimpleIndexBuilder<K, V> keyTypeDescriptor(final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = keyTypeDescriptor;
        return this;
    }

    public SimpleIndexBuilder<K, V> valueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = valueTypeDescriptor;
        return this;
    }

    public SimpleIndexBuilder<K, V> valueMerger(final ValueMerger<K, V> valueMerger) {
        this.valueMerger = valueMerger;
        return this;
    }

}
