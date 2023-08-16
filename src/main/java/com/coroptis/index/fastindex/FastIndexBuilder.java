package com.coroptis.index.fastindex;

import java.util.Objects;

import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

public class FastIndexBuilder<K, V> {

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE = 1000 * 1000
            * 10;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 1000
            * 1000 * 10;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT = 1000 * 1000
            * 100;

    FastIndexBuilder() {
    }

    private long maxNumberOfKeysInCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_CACHE;

    private long maxNumeberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;

    private long maxNumeberOfKeysInSegment = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT;

    private Directory directory;

    private TypeDescriptor<K> keyTypeDescriptor;

    private TypeDescriptor<V> valueTypeDescriptor;

    private ValueMerger<K, V> valueMerger;

    public FastIndexBuilder<K, V> withMaxNumberOfKeysInCache(
            final long maxNumberOfKeysInCache) {
        this.maxNumberOfKeysInCache = Objects
                .requireNonNull(maxNumberOfKeysInCache);
        return this;
    }

    public FastIndexBuilder<K, V> withMaxNumeberOfKeysInSegmentCache(
            final long maxNumeberOfKeysInSegmentCache) {
        this.maxNumeberOfKeysInSegmentCache = Objects
                .requireNonNull(maxNumeberOfKeysInSegmentCache);
        return this;
    }

    public FastIndexBuilder<K, V> withMaxNumeberOfKeysInSegment(
            final long maxNumeberOfKeysInSegment) {
        this.maxNumeberOfKeysInSegment = Objects
                .requireNonNull(maxNumeberOfKeysInSegment);
        return this;
    }

    public FastIndexBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public FastIndexBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public FastIndexBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public FastIndexBuilder<K, V> withValueMerger(
            final ValueMerger<K, V> valueMerger) {
        this.valueMerger = Objects.requireNonNull(valueMerger);
        return this;
    }

    public FastIndex<K, V> build() {
        return new FastIndex<>(directory, keyTypeDescriptor,
                valueTypeDescriptor, valueMerger, maxNumberOfKeysInCache,
                maxNumeberOfKeysInSegmentCache, maxNumeberOfKeysInSegment);
    }

}
