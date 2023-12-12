package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

public class SegmentBuilder<K, V> {

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 1000
            * 1000 * 10;

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_INDEX_PAGE = 1000;

    private final static int DEFAULT_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = 1_000;
    private final static int DEFAULT_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = 10_000;

    private Directory directory;
    private SegmentId id;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private long maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private int maxNumberOfKeysInIndexPage = DEFAULT_MAX_NUMBER_OF_KEYS_IN_INDEX_PAGE;
    private int bloomFilterNumberOfHashFunctions = DEFAULT_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    private int bloomFilterIndexSizeInBytes = DEFAULT_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;

    SegmentBuilder() {

    }

    public SegmentBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public SegmentBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public SegmentBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public SegmentBuilder<K, V> withId(final Integer id) {
        this.id = SegmentId.of(Objects.requireNonNull(id));
        return this;
    }

    public SegmentBuilder<K, V> withId(final SegmentId id) {
        this.id = Objects.requireNonNull(id);
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final long maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = Objects
                .requireNonNull(maxNumberOfKeysInSegmentCache);
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInIndexPage(
            final int maxNumberOfKeysInIndexPage) {
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        return this;
    }

    public SegmentBuilder<K, V> withBloomFilterNumberOfHashFunctions(
            final int bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        return this;
    }

    public SegmentBuilder<K, V> withBloomFilterIndexSizeInBytes(
            final int bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        return this;
    }

    public Segment<K, V> build() {
        return new Segment<>(directory, id, maxNumberOfKeysInSegmentCache,
                keyTypeDescriptor, valueTypeDescriptor,
                maxNumberOfKeysInIndexPage, bloomFilterNumberOfHashFunctions,
                bloomFilterIndexSizeInBytes);
    }

}
