package com.coroptis.index.sst;

public class SsstIndexConf {

    /*
     * Segments configuration
     */

    private final long maxNumberOfKeysInSegmentCache;
    private final int maxNumberOfKeysInSegmentIndexPage;

    /*
     * SST index configuration
     */

    private final int maxNumberOfKeysInCache;
    private final int maxNumberOfKeysInSegment;

    SsstIndexConf(final long maxNumberOfKeysInSegmentCache,
            final int maxNumberOfKeysInSegmentIndexPage,
            final int maxNumberOfKeysInCache,
            final int maxNumberOfKeysInSegment) {
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentIndexPage = maxNumberOfKeysInSegmentIndexPage;
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
    }

    public long getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    public int getMaxNumberOfKeysInSegmentIndexPage() {
        return maxNumberOfKeysInSegmentIndexPage;
    }

    public long getMaxNumberOfKeysInCache() {
        return maxNumberOfKeysInCache;
    }

    public int getMaxNumberOfKeysInSegment() {
        return maxNumberOfKeysInSegment;
    }

}
