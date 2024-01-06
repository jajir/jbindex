package com.coroptis.index.sst;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentFiles;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.segment.SegmentStatsManager;
import com.coroptis.index.segment.SegmentWriter;
import com.coroptis.index.segmentcache.SegmentCacheManager;
import com.coroptis.index.segmentcache.SegmentCompacter;

public class CompactSupport<K, V> {

    private final Logger logger = LoggerFactory.getLogger(CompactSupport.class);

    private final List<Pair<K, V>> toSameSegment = new ArrayList<>();
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentManager<K, V> segmentManager;
    private final long maxNumberOfKeysInSegmentCache;
    private SegmentId currentSegmentId = null;

    /**
     * List of segment's ids eligible for compacting.
     */
    private List<SegmentId> eligibleSegments = new ArrayList<>();

    CompactSupport(final SegmentManager<K, V> segmentManager,
            final KeySegmentCache<K> keySegmentCache,
            final long maxNumberOfKeysInSegmentCache) {
        this.segmentManager = Objects.requireNonNull(segmentManager);
        this.keySegmentCache = Objects.requireNonNull(keySegmentCache);
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
    }

    public void compact(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        final K segmentKey = pair.getKey();
        final SegmentId segmentId = keySegmentCache
                .insertKeyToSegment(segmentKey);
        if (currentSegmentId == null) {
            currentSegmentId = segmentId;
            toSameSegment.add(pair);
            return;
        }
        if (currentSegmentId == segmentId) {
            toSameSegment.add(pair);
            return;
        } else {
            /* Write all keys to index and clean cache and set new pageId */
            flushToCurrentSegment();
            toSameSegment.add(pair);
            currentSegmentId = segmentId;
        }
    }

    public void compactRest() {
        if (currentSegmentId == null) {
            return;
        }
        flushToCurrentSegment();
        currentSegmentId = null;
    }

    private void flushToCurrentSegment() {
        logger.debug("Flushing '{}' key value pairs into segment '{}'.",
                toSameSegment.size(), currentSegmentId);
//        if (segmentManager.isInCache(currentSegmentId)) {
//            final Segment<K, V> segment = segmentManager
//                    .getSegment(currentSegmentId);
//            try (final SegmentWriter<K, V> writer = segment.openWriter()) {
//                toSameSegment.forEach(writer::put);
//            }
//        } else {
            final SegmentFiles<K, V> segmentFiles = segmentManager
                    .getSegmentFiles(currentSegmentId);
            final SegmentStatsManager segmentStatsManager = new SegmentStatsManager(
                    segmentManager.getDirectory(), currentSegmentId);
            final SegmentCacheManager<K, V> segmentCacheManager = new SegmentCacheManager<>(
                    segmentManager, segmentFiles, segmentStatsManager);
            try (final PairWriter<K, V> writer = segmentCacheManager
                    .openWriter()) {
                toSameSegment.forEach(writer::put);
            }

            final SegmentCompacter<K, V> segmentCompacter = new SegmentCompacter<>(
                    segmentStatsManager, maxNumberOfKeysInSegmentCache,
                    segmentManager, segmentFiles.getId());
            segmentCompacter.flush();
//        }
        eligibleSegments.add(currentSegmentId);
        toSameSegment.clear();
        logger.debug("Flushing to segment '{}' was done.", currentSegmentId);
    }

    /**
     * After compacting all keys to appropriate segment it allows to obtain list
     * of that segment.
     * 
     * @return list of segment eligible form compacting
     */
    public List<SegmentId> getEligibleSegmentIds() {
        return eligibleSegments;
    }

}
