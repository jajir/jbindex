package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

/**
 * Object use in memory cache and bloom filter. Only one instance for one
 * segment should be in memory at the time.
 * 
 * This object can be cached in memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSearcherCore<K, V> implements CloseableResource {

    private final SegmentDeltaCache<K, V> deltaCache;
    private final ScarceIndex<K> scarceIndex;
    private final BloomFilter<K> bloomFilter;
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentIndexSearcher<K, V> segmentIndexSearcher;
    private final SegmentCacheDataProvider<K, V> segmentCacheDataProvider;

    public SegmentSearcherCore(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher,
            final SegmentCacheDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
        this.deltaCache = new SegmentDeltaCache<>(
                segmentFiles.getKeyTypeDescriptor(), segmentFiles,
                segmentPropertiesManager);
        this.scarceIndex = ScarceIndex.<K>builder()
                .withDirectory(segmentFiles.getDirectory())
                .withFileName(segmentFiles.getScarceFileName())
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .build();
        this.bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(segmentFiles.getKeyTypeDescriptor()
                        .getConvertorToBytes())
                .withDirectory(segmentFiles.getDirectory())
                .withIndexSizeInBytes(
                        segmentConf.getBloomFilterIndexSizeInBytes())
                .withNumberOfHashFunctions(
                        segmentConf.getBloomFilterNumberOfHashFunctions())
                .build();
        this.segmentIndexSearcher = Objects
                .requireNonNull(segmentIndexSearcher);
    }

    public K getMaxKey() {
        return scarceIndex.getMaxKey();
    }

    public K getMinKey() {
        return scarceIndex.getMinKey();
    }

    public V get(final K key) {
        // look in cache
        final V out = deltaCache.get(key);
        if (segmentFiles.getValueTypeDescriptor().isTombstone(out)) {
            return null;
        }

        // look in bloom filter
        if (out == null) {
            if (bloomFilter.isNotStored(key)) {
                /*
                 * It;s sure that key is not in index.
                 */
                return null;
            }
        }

        // look in index file
        if (out == null) {
            final Integer position = scarceIndex.get(key);
            if (position == null) {
                return null;
            }
            return segmentIndexSearcher.search(key, position);
        }
        return out;
    }

    void addPairIntoCache(final Pair<K, V> pair) {
        deltaCache.put(pair);
    }

    BloomFilter<K> getBloomFilter() {
        return bloomFilter;
    }

    SegmentDeltaCache<K, V> getCache() {
        return deltaCache;
    }

    @Override
    public void close() {
        segmentIndexSearcher.close();
    }

}
