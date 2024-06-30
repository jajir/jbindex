package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.datatype.TypeDescriptor;
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
public class SegmentSearcher<K, V> implements CloseableResource {

    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentIndexSearcher<K, V> segmentIndexSearcher;
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;

    public SegmentSearcher(final TypeDescriptor<V> valueTypeDescriptor,
            final SegmentConf segmentConf,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher,
            final SegmentDataProvider<K, V> segmentDataProvider) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentDataProvider,
                "Segment cached data provider is required");
        this.segmentIndexSearcher = Objects
                .requireNonNull(segmentIndexSearcher);
    }

    private SegmentDeltaCache<K, V> getDeltaCache() {
        return segmentCacheDataProvider.getSegmentDeltaCache();
    };

    private ScarceIndex<K> getScarceIndex() {
        return segmentCacheDataProvider.getScarceIndex();
    };

    private BloomFilter<K> getBloomFilter() {
        return segmentCacheDataProvider.getBloomFilter();
    }

    public V get(final K key) {
        // look in cache
        final V out = getDeltaCache().get(key);
        if (valueTypeDescriptor.isTombstone(out)) {
            return null;
        }

        // look in bloom filter
        if (out == null) {
            if (getBloomFilter().isNotStored(key)) {
                /*
                 * It;s sure that key is not in index.
                 */
                return null;
            }
        }

        // look in index file
        if (out == null) {
            final Integer position = getScarceIndex().get(key);
            if (position == null) {
                return null;
            }
            return segmentIndexSearcher.search(key, position);
        }
        return out;
    }

    @Override
    public void close() {
        segmentIndexSearcher.close();
    }

}
