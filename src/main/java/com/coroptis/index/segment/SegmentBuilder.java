package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

public class SegmentBuilder<K, V> {

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 1000
            * 1000 * 10;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE
            * 5;

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_INDEX_PAGE = 1000;

    private final static int DEFAULT_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = 6;
    private final static int DEFAULT_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = 10_000;
    private final static int DEFAUL_MAX_NUMBER_OF_KEYS_IN_SEGMENT_MEMORY = Integer.MAX_VALUE;

    private final static int DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES = 1024 * 4;

    private Directory directory;
    private SegmentId id;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private long maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private long maxNumberOfKeysInSegmentCacheDuringFlushing = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING;
    private int maxNumberOfKeysInIndexPage = DEFAULT_MAX_NUMBER_OF_KEYS_IN_INDEX_PAGE;
    private int bloomFilterNumberOfHashFunctions = DEFAULT_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    private int bloomFilterIndexSizeInBytes = DEFAULT_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    private long maxNumberOfKeysInSegmentMemory = DEFAUL_MAX_NUMBER_OF_KEYS_IN_SEGMENT_MEMORY;
    private VersionController versionController;
    private SegmentConf segmentConf;
    private SegmentFiles<K, V> segmentFiles;
    private SegmentDataProvider<K, V> segmentDataProvider;
    private int indexBufferSizeInBytes = DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES;

    SegmentBuilder() {

    }

    public SegmentBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public SegmentBuilder<K, V> withSegmentConf(final SegmentConf segmentConf) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        return this;
    }

    public SegmentBuilder<K, V> withSegmentFiles(
            final SegmentFiles<K, V> segmentFiles) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
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

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentCacheDuringFlushing(
            final long maxNumberOfKeysInSegmentCacheDuringFlushing) {
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = Objects
                .requireNonNull(maxNumberOfKeysInSegmentCacheDuringFlushing);
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInIndexPage(
            final int maxNumberOfKeysInIndexPage) {
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentMemory(
            final long maxNumberOfKeysInSegmentMemory) {
        this.maxNumberOfKeysInSegmentMemory = Objects
                .requireNonNull(maxNumberOfKeysInSegmentMemory);
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

    public SegmentBuilder<K, V> withVersionController(
            final VersionController versionController) {
        this.versionController = versionController;
        return this;
    }

    public SegmentBuilder<K, V> withSegmentDataProvider(
            final SegmentDataProvider<K, V> segmentDataProvider) {
        this.segmentDataProvider = segmentDataProvider;
        return this;
    }

    public SegmentBuilder<K, V> withIndexBufferSizeInBytes(
            final int indexBufferSizeInBytes) {
        this.indexBufferSizeInBytes = indexBufferSizeInBytes;
        return this;
    }

    public Segment<K, V> build() {
        if (versionController == null) {
            versionController = new VersionController();
        }
        if (segmentConf == null) {
            segmentConf = new SegmentConf(maxNumberOfKeysInSegmentCache,
                    maxNumberOfKeysInSegmentCacheDuringFlushing,
                    maxNumberOfKeysInIndexPage,
                    bloomFilterNumberOfHashFunctions,
                    bloomFilterIndexSizeInBytes,
                    maxNumberOfKeysInSegmentMemory);
        }
        if (segmentFiles == null) {
            segmentFiles = new SegmentFiles<>(directory, id, keyTypeDescriptor,
                    valueTypeDescriptor, indexBufferSizeInBytes);
        }
        final SegmentPropertiesManager segmentPropertiesManager = new SegmentPropertiesManager(
                segmentFiles.getDirectory(), id);
        if (segmentDataProvider == null) {
            SegmentDataDirectLoader<K, V> directLoader = new SegmentDataDirectLoader<>(
                    segmentFiles, segmentConf, segmentPropertiesManager);
            segmentDataProvider = new SegmentDataProviderSimple<>(directLoader);
        }
        final SegmentIndexSearcherSupplier<K, V> supplier = new SegmentIndexSearcherDefaultSupplier<>(
                segmentFiles, segmentConf);
        final SegmentSearcher<K, V> segmentSearcher = new SegmentSearcher<K, V>(
                segmentFiles.getValueTypeDescriptor(), segmentConf,
                segmentPropertiesManager, supplier.get(), segmentDataProvider);

        return new Segment<>(segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentDataProvider, segmentSearcher);
    }

}
