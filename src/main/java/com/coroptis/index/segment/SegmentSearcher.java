package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
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
public class SegmentSearcher<K, V> implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(Segment.class);
    private final SegmentCache<K, V> cache;
    private final ScarceIndex<K> scarceIndex;
    private final BloomFilter<K> bloomFilter;
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;

    public SegmentSearcher(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.cache = new SegmentCache<>(segmentFiles.getKeyTypeDescriptor(),
                segmentFiles);
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
    }

    public K getMaxKey() {
        return scarceIndex.getMaxKey();
    }

    public K getMinKey() {
        return scarceIndex.getMinKey();
    }

    public V get(final K key) {
        // look in cache
        final V out = cache.get(key);
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
            try (final PairReader<K, V> fileReader = segmentFiles
                    .getIndexSstFile().openReader(position)) {
                for (int i = 0; i < segmentConf
                        .getMaxNumberOfKeysInIndexPage(); i++) {
                    final Pair<K, V> pair = fileReader.read();
                    final int cmp = segmentFiles.getKeyTypeDescriptor()
                            .getComparator().compare(pair.getKey(), key);
                    if (cmp == 0) {
                        return pair.getValue();
                    }
                    /**
                     * Keys are in ascending order. When searched key is smaller
                     * than key read from sorted data than key is not found.
                     */
                    if (cmp > 0) {
                        return null;
                    }
                }
            }
        }
        return out;
    }

    @Override
    public void close() {
        bloomFilter.logStats();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
        // Do intentionally nothing.
    }

}
