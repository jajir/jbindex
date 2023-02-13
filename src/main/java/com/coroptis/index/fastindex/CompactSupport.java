package com.coroptis.index.fastindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.simpledatafile.SimpleDataFile;

public class CompactSupport<K, V> {

	Logger logger = LoggerFactory.getLogger(CompactSupport.class);

    private final List<Pair<K, V>> toSameSegment = new ArrayList<>();
    private final FastIndexFile<K> fastIndexFile;
    private final FastIndex<K, V> fastIndex;
    K currentSegmentKey = null;
    int currentSegmentId = -1;

    CompactSupport(final FastIndex<K, V> fastIndex, final FastIndexFile<K> fastIndexFile) {
	this.fastIndex = fastIndex;
	this.fastIndexFile = fastIndexFile;
    }

    public void compact(final Pair<K, V> pair) {
	Objects.requireNonNull(pair);
	final K segmentKey = pair.getKey();
	final int pageId = fastIndexFile.insertKeyToSegment(segmentKey);
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
	    flushToCurrentPageIdSegment();
	    toSameSegment.add(pair);
	    currentSegmentId = pageId;
	}
    }

    public void compactRest() {
	if (currentSegmentId == -1) {
	    return;
	}
	flushToCurrentPageIdSegment();
	currentSegmentId = -1;
    }

    private void flushToCurrentPageIdSegment() {
	logger.debug("Flushing '{}' key value pairs into segment '{}'.",  toSameSegment.size(),  currentSegmentId);
	final SimpleDataFile<K, V> sdf = fastIndex.getSegment(currentSegmentId);
	try (final PairWriter<K, V> writer = sdf.openCacheWriter()) {
	    toSameSegment.forEach(writer::put);
	}
	toSameSegment.clear();
	logger.debug("Flushing to segment '{}' was done.",  currentSegmentId);
    }

}
