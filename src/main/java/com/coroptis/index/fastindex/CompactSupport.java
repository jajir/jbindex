package com.coroptis.index.fastindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairFileWriter;
import com.coroptis.index.simpledatafile.SimpleDataFile;

public class CompactSupport<K, V> {

    private final List<Pair<K, V>> toSameSegment = new ArrayList<>();
    private final FastIndexFile<K> fastIndexFile;
    private final FastIndex<K, V> fastIndex;
    K currentSegmentKey = null;
    int currentPageId = -1;

    CompactSupport(final FastIndex<K, V> fastIndex, final FastIndexFile<K> fastIndexFile) {
	this.fastIndex = fastIndex;
	this.fastIndexFile = fastIndexFile;
    }

    public void compact(final Pair<K, V> pair) {
	Objects.requireNonNull(pair);
	final K segmentKey = pair.getKey();
	final int pageId = fastIndexFile.insertKeyToPage(segmentKey);
	if (currentPageId == -1) {
	    currentPageId = pageId;
	    toSameSegment.add(pair);
	    return;
	}
	if (currentPageId == pageId) {
	    toSameSegment.add(pair);
	    return;
	} else {
	    /* Write all keys to index and clean cache and set new pageId */
	    flushToCurrentPageIdSegment();
	    toSameSegment.add(pair);
	    currentPageId = pageId;
	}
    }

    public void compactRest() {
	if (currentPageId == -1) {
	    return;
	}
	flushToCurrentPageIdSegment();
	currentPageId = -1;
    }

    private void flushToCurrentPageIdSegment() {
	System.out.println("Flushig data " + toSameSegment.size() + " to segment " + currentPageId);
	final SimpleDataFile<K, V> sdf = fastIndex.getSegment(currentPageId);
	try (final PairFileWriter<K, V> writer = sdf.openCacheWriter()) {
	    toSameSegment.forEach(writer::put);
	}
	toSameSegment.clear();
    }

}
