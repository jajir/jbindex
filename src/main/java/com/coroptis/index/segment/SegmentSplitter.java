package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.bloomfilter.BloomFilter;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSplitter<K, V> {

    private final Logger logger = LoggerFactory
            .getLogger(SegmentSplitter.class);
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;

    public static class Result<K, V> {

        private final Segment<K, V> segment;
        private final K maxKey;
        private final K minKey;

        private Result(final Segment<K, V> segment, final K minKey,
                final K maxKey) {
            this.segment = Objects.requireNonNull(segment);
            this.minKey = Objects.requireNonNull(minKey);
            this.maxKey = Objects.requireNonNull(maxKey);
        }

        public Segment<K, V> getSegment() {
            return segment;
        }

        public K getMaxKey() {
            return maxKey;
        }

        public K getMinKey() {
            return minKey;
        }

    }

    public SegmentSplitter(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
    }

    private SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    private PairIterator<K, V> openIterator() {
        return new SegmentReader<>(segmentFiles, segmentPropertiesManager)
                .openIterator(versionController);
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file. It's useful for
     * compacting.
     */
    private SegmentFullWriter<K, V> openFullWriter() {
        final BloomFilter<K> bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(segmentFiles.getKeyTypeDescriptor()
                        .getConvertorToBytes())
                .withDirectory(segmentFiles.getDirectory())
                .withIndexSizeInBytes(
                        segmentConf.getBloomFilterIndexSizeInBytes())
                .withNumberOfHashFunctions(
                        segmentConf.getBloomFilterNumberOfHashFunctions())
                .build();
        return new SegmentFullWriter<K, V>(bloomFilter, segmentFiles,
                segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInIndexPage());
    }

    public Result<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        logger.debug("Start of splitting '{}'", segmentFiles.getId());
        versionController.changeVersion();
        long cx = 0;
        long half = getStats().getNumberOfKeys() / 2;

        final Segment<K, V> lowerSegment = Segment.<K, V>builder()
                .withDirectory(segmentFiles.getDirectory()).withId(segmentId)
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())
                .withSegmentConf(segmentConf).build();

        K minKey = null;
        K maxKey = null;
        try (PairIterator<K, V> iterator = openIterator()) {

            try (SegmentFullWriter<K, V> writer = lowerSegment
                    .openFullWriter()) {
                while (cx < half && iterator.hasNext()) {
                    cx++;
                    final Pair<K, V> pair = iterator.next();
                    if (minKey == null) {
                        minKey = pair.getKey();
                    }
                    maxKey = pair.getKey();
                    writer.put(pair);
                }
            }

            try (SegmentFullWriter<K, V> writer = openFullWriter()) {
                while (iterator.hasNext()) {
                    final Pair<K, V> pair = iterator.next();
                    writer.put(pair);
                }
            }

        }
        logger.debug("End of splitting '{}', '{}' was created",
                segmentFiles.getId(), lowerSegment.getId());
        return new Result<>(lowerSegment, minKey, maxKey);
    }

}
