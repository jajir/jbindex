package com.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hestiastore.index.AbstractDataTest;
import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.PairWriter;
import com.hestiastore.index.directory.Directory;

public abstract class AbstractSegmentTest extends AbstractDataTest {

    private final Logger logger = LoggerFactory
            .getLogger(AbstractSegmentTest.class);

    /**
     * Simplify filling segment with data.
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of pairs
     */
    protected <M, N> void writePairs(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        try (PairWriter<M, N> writer = seg.openWriter()) {
            for (final Pair<M, N> pair : pairs) {
                writer.put(pair);
            }
        }
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expected value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of pairs of key and expected value
     */
    protected <M, N> void verifySegmentSearch(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        pairs.forEach(pair -> {
            final M key = pair.getKey();
            final N expectedValue = pair.getValue();
            assertEquals(expectedValue, seg.get(key));
        });
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expecetd value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of expected data in segment
     */
    protected <M, N> void verifySegmentData(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        verifyIteratorData(seg.openIterator(), pairs);
    }

    /**
     * Verify that data from iterator are same as expecetd values
     * 
     * @param <M>          key type
     * @param <N>          value type
     * @param pairIterator required pair iterator
     * @param pairs        required list of expected data in segment
     */
    protected <M, N> void verifyIteratorData(
            final PairIterator<M, N> pairIterator,
            final List<Pair<M, N>> pairs) {
        final List<Pair<M, N>> data = toList(pairIterator);
        assertEquals(pairs.size(), data.size(), "Unexpected segment data size");
        for (int i = 0; i < pairs.size(); i++) {
            final Pair<M, N> expectedPair = pairs.get(i);
            final Pair<M, N> realPair = data.get(i);
            assertEquals(expectedPair, realPair);
        }
    }

    protected int numberOfFilesInDirectory(final Directory directory) {
        return (int) directory.getFileNames().count();
    }

    protected int numberOfFilesInDirectoryP(final Directory directory) {
        final AtomicInteger cx = new AtomicInteger(0);
        directory.getFileNames().forEach(fileName -> {
            logger.debug("Found file name {}", fileName);
            cx.incrementAndGet();
        });
        return cx.get();
    }

    protected void verifyCacheFiles(final Directory directory) {
        long cacheFileCount = directory.getFileNames()
                .filter(fileName -> fileName.endsWith(".cache")).count();
        assertEquals(0, cacheFileCount,
                "Expected zero .cache files in directory");
    }

    protected void verifyNumberOfFiles(final Directory directory,
            final int expecetdNumberOfFiles) {
        long sileCount = directory.getFileNames().count();
        assertEquals(expecetdNumberOfFiles, sileCount,
                "Invalid numbe of files in directory");
    }

}
