package com.coroptis.index.fastindex;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.partiallysorteddatafile.UniqueCache;
import com.coroptis.index.simpledatafile.MergedPairReader;
import com.coroptis.index.simpledatafile.SortedStringTable;
import com.coroptis.index.sst.PairComparator;
import com.coroptis.index.type.TypeDescriptor;

/**
 * Main end user class.
 * 
 * 
 * Consider moving writing to writer and reading to reader.
 * 
 * 
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class FastIndex<K, V> implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(FastIndex.class);

    private final long maxNumberOfKeysInCache;
    private final long maxNumeberOfKeysInSegmentCache;
    private final long maxNumeberOfKeysInSegment;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ValueMerger<K, V> valueMerger;
    private final ScarceIndexFile<K> scarceIndexFile;
    private final UniqueCache<K, V> cache;

    public static <M, N> FastIndexBuilder<M, N> builder() {
        return new FastIndexBuilder<>();
    }

    public FastIndex(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final ValueMerger<K, V> valueMerger,
            final long maxNumberOfKeysInCache,
            final long maxNumeberOfKeysInSegmentCache,
            final long maxNumeberOfKeysInSegment) {
        this.maxNumberOfKeysInCache = Objects
                .requireNonNull(maxNumberOfKeysInCache);
        this.maxNumeberOfKeysInSegmentCache = Objects
                .requireNonNull(maxNumeberOfKeysInSegmentCache);
        this.maxNumeberOfKeysInSegment = Objects
                .requireNonNull(maxNumeberOfKeysInSegment);
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.valueMerger = Objects.requireNonNull(valueMerger);
        this.scarceIndexFile = new ScarceIndexFile<>(directory,
                keyTypeDescriptor);
        this.cache = new UniqueCache<>(valueMerger,
                keyTypeDescriptor.getComparator());
    }

    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        cache.add(pair);
        if (cache.size() > maxNumberOfKeysInCache) {
            compact();
        }
    }

    public void forceCompactCache() {
        /*
         * Empty cache.
         */
        compact();
    }

    public void forceCompactSegments() {
        compact();
        /*
         * Defensive copy have to be done, because further splitting will affect
         * list size. In the future it will be slow.
         */
        final List<Integer> eligibleSegmentIds = scarceIndexFile
                .getSegmentsAsStream().map(Pair::getValue)
                .collect(Collectors.toList());
        compactSegmenst(eligibleSegmentIds);
    }

    private void compact() {
        logger.debug(
                "Cache compacting of '{}' key value pairs in cache started.",
                cache.size());
        final CompactSupport<K, V> support = new CompactSupport<>(this,
                scarceIndexFile);
        cache.getStream()
                .sorted(new PairComparator<>(keyTypeDescriptor.getComparator()))
                .forEach(support::compact);
        support.compactRest();
        cache.clear();
        scarceIndexFile.flush();
        logger.debug(
                "Cache compacting is done. Cache contains '{}' key value pairs.",
                cache.size());
        tryToCopactsSegmenst(support.getEligibleSegmentIds());
    }

    SortedStringTable<K, V> getSegment(final SegmentId segmentId) {
        final SortedStringTable<K, V> sst = SortedStringTable.make(directory,
                segmentId.getName(), keyTypeDescriptor, valueTypeDescriptor,
                valueMerger);
        return sst;
    }

    /**
     * Verify that number of keys in segments doesn't exceed some threshold.
     * When it exceed than segment is merged or split into two smaller segment.
     */
    public void optionallyCompactSegments() {
        /*
         * Defensive copy have to be done, because further splitting will affect
         * list size. In the future it will be slow.
         */
        final List<SegmentId> eligibleSegmentIds = scarceIndexFile
                .getSegmentsAsStream().map(Pair::getValue).map(SegmentId::of)
                .collect(Collectors.toList());
        tryToCopactsSegmenst(eligibleSegmentIds);
    }

    private void tryToCopactsSegmenst(final List<SegmentId> eligibleSegment) {
        Objects.requireNonNull(eligibleSegment);
        logger.debug("Start of compacting of '{}' segments.",
                eligibleSegment.size());
        eligibleSegment.forEach(this::optionallyCompactSegment);
        scarceIndexFile.flush();
        logger.debug("Compacting of '{}' segments is done.",
                eligibleSegment.size());
    }

    private void optionallyCompactSegment(final SegmentId segmentId) {
        final SortedStringTable<K, V> sdf = getSegment(segmentId);
        optionallySplit(sdf, segmentId);
        if (sdf.getStats()
                .getNumberOfPairsInCache() > maxNumeberOfKeysInSegmentCache) {
            logger.debug("Compacting of segment '{}' started.", segmentId);
            sdf.compact();
            logger.debug("Compacting of segment '{}' is done.", segmentId);
        }
    }

    /**
     * If number of keys reach threshold split segment into two.
     * 
     * @param sdf       required simple data file
     * @param segmentId required segment id
     * @return
     */
    private boolean optionallySplit(final SortedStringTable<K, V> sdf,
            final SegmentId segmentId) {
        if (sdf.getStats()
                .getNumberOfPairsInMainFile() > maxNumeberOfKeysInSegment) {
            logger.debug("Splitting of '{}' started.", segmentId);
            final SegmentId newSegmentId = scarceIndexFile.findNewSegmentId();
            final K newPageKey = sdf.split(newSegmentId.getName());
            scarceIndexFile.insertSegment(newPageKey, newSegmentId);
            logger.debug("Splitting of segment '{}' to '{}' is done.",
                    segmentId, newSegmentId);
            return true;
        }
        return false;
    }

    private void compactSegmenst(final List<Integer> eligibleSegment) {
        Objects.requireNonNull(eligibleSegment);
        logger.debug("Start of force compacting of '{}' segments.",
                eligibleSegment.size());
        eligibleSegment.forEach(segmentId -> {
            final SortedStringTable<K, V> sdf = getSegment(
                    SegmentId.of(segmentId));
            if (sdf.getStats().getNumberOfPairsInCache() > 0) {
                logger.debug("Compacting of segment '{}' started.", segmentId);
                sdf.compact();
                logger.debug("Compacting of segment '{}' is done.", segmentId);
            }
        });
        logger.debug("Force compacting of '{}' segments is done.",
                eligibleSegment.size());
    }

    /**
     * It allows to iterate over all stored data in sorted way.
     * 
     * @return
     */
    public PairReader<K, V> openReader() {
        final FastIndexReader<K, V> fastIndexreader = new FastIndexReader<>(
                this, scarceIndexFile);
        final PairReader<K, V> cacheReader = cache.openSortedClonedReader();
        final MergedPairReader<K, V> mergedPairReader = new MergedPairReader<>(
                cacheReader, fastIndexreader, valueMerger,
                keyTypeDescriptor.getComparator());
        return mergedPairReader;
    }

    @Override
    public void close() {
        compact();
        scarceIndexFile.close();
    }

}
