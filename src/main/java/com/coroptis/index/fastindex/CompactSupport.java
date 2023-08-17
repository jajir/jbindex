package com.coroptis.index.fastindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.simpledatafile.SortedStringTable;

public class CompactSupport<K, V> {

    private final Logger logger = LoggerFactory.getLogger(CompactSupport.class);

    private final List<Pair<K, V>> toSameSegment = new ArrayList<>();
    private final ScarceIndexFileOld<K> scarceIndexFile;
    private final FastIndex<K, V> fastIndex;
    private int currentSegmentId = -1;
    /**
     * List of segment's ids eligible for compacting.
     */
    private List<SegmentId> eligibleSegments = new ArrayList<>();

    CompactSupport(final FastIndex<K, V> fastIndex,
            final ScarceIndexFileOld<K> scarceIndexFile) {
        this.fastIndex = fastIndex;
        this.scarceIndexFile = scarceIndexFile;
    }

    public void compact(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        final K segmentKey = pair.getKey();
        final int pageId = scarceIndexFile.insertKeyToSegment(segmentKey);
        if (currentSegmentId == -1) {
            currentSegmentId = pageId;
            toSameSegment.add(pair);
            return;
        }
        if (currentSegmentId == pageId) {
            toSameSegment.add(pair);
            return;
        } else {
            /* Write all keys to index and clean cache and set new pageId */
            flushToCurrentSegment();
            toSameSegment.add(pair);
            currentSegmentId = pageId;
        }
    }

    public void compactRest() {
        if (currentSegmentId == -1) {
            return;
        }
        flushToCurrentSegment();
        currentSegmentId = -1;
    }

    private void flushToCurrentSegment() {
        logger.debug("Flushing '{}' key value pairs into segment '{}'.",
                toSameSegment.size(), currentSegmentId);
        final SortedStringTable<K, V> sdf = fastIndex
                .getSegment(SegmentId.of(currentSegmentId));
        try (final PairWriter<K, V> writer = sdf.openCacheWriter()) {
            toSameSegment.forEach(writer::put);
        }
        eligibleSegments.add(SegmentId.of(currentSegmentId));
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
