package com.coroptis.index.segment;

/**
 * Provide some basic statistic information about segment.
 * 
 * @author honza
 *
 */
public class SegmentStats {

    private final long numberOfKeysInCache;
    private final long numberOfKeysInIndex;

    SegmentStats(final long numberOfKeysInCache,
            final long numberOfKeysInIndex) {
        this.numberOfKeysInCache = numberOfKeysInCache;
        this.numberOfKeysInIndex = numberOfKeysInIndex;
    }

    public long getNumberOfKeysInCache() {
        return numberOfKeysInCache;
    }

    public long getNumberOfKeysInIndex() {
        return numberOfKeysInIndex;
    }

    public long getNumberOfKeys() {
        return getNumberOfKeysInCache() + getNumberOfKeysInIndex();
    }
}
