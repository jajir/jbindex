package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;

/**
 * Allows to add data to segment. When searcher is in memory and number of added
 * keys doesn't exceed limit than it could work without invalidating cache and
 * searcher object..
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentWriter<K, V> implements PairWriter<K, V> {

    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    /**
     * holds current delta cache writer.
     */
    private SegmentDeltaCacheWriter<K, V> deltaCacheWriter;

    public SegmentWriter(final SegmentCompacter<K, V> segmentCompacter,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentCompacter = Objects.requireNonNull(segmentCompacter);
        this.deltaCacheController = Objects
                .requireNonNull(deltaCacheController);
    }

    @Override
    public void close() {
        if (deltaCacheWriter != null) {
            deltaCacheWriter.close();
            //FIXME compacting is not decisioon of writer, but calling code should decide
            if (segmentCompacter
                    .shouldBeCompacted(deltaCacheWriter.getNumberOfKeys())) {
                deltaCacheWriter = null;
                segmentCompacter.forceCompact();
            }
        }
    }

    @Override
    public void put(Pair<K, V> pair) {
        optionallyOpenDeltaCacheWriter();
        deltaCacheWriter.put(pair);
        if (segmentCompacter.shouldBeCompactedDuringFlushing(
                deltaCacheWriter.getNumberOfKeys())) {
            deltaCacheWriter.close();
            deltaCacheWriter = null;
            segmentCompacter.forceCompact();
        }
    }

    private void optionallyOpenDeltaCacheWriter() {
        if (deltaCacheWriter == null) {
            deltaCacheWriter = deltaCacheController.openWriter();
        }
    }

}
