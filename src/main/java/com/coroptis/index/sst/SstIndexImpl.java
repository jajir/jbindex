package com.coroptis.index.sst;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.sstfile.PairComparator;

public class SstIndexImpl<K, V> implements Index<K, V>, CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(SstIndexImpl.class);

    private final SsstIndexConf conf;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;
    private final SegmentCache<K> segmentCache;

    public SstIndexImpl(final Directory directory,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor, final SsstIndexConf conf) {
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.conf = Objects.requireNonNull(conf);
        this.cache = new UniqueCache<K, V>(
                this.keyTypeDescriptor.getComparator());
        this.segmentCache = new SegmentCache<>(directory, keyTypeDescriptor);
    }

    @Override
    public void put(final K key, final V value) {
        Objects.requireNonNull(key, "Key cant be null");
        Objects.requireNonNull(value, "Value cant be null");

        if (valueTypeDescriptor.isTombstone(value)) {
            throw new IllegalArgumentException(String.format(
                    "Can't insert thombstone value '%s' into index", value));
        }

        // TODO add key value pair into WAL

        cache.put(Pair.of(key, value));

        if (cache.size() > conf.getMaxNumberOfKeysInCache()) {
            compact();
        }
    }

    private void compact() {
        logger.debug(
                "Cache compacting of '{}' key value pairs in cache started.",
                cache.size());
        final CompactSupport<K, V> support = new CompactSupport<>(this,
                segmentCache);
        cache.getStream()
                .sorted(new PairComparator<>(keyTypeDescriptor.getComparator()))
                .forEach(support::compact);
        support.compactRest();
        cache.clear();
        segmentCache.flush();
        logger.debug(
                "Cache compacting is done. Cache contains '{}' key value pairs.",
                cache.size());
    }

    Segment<K, V> getSegment(final SegmentId segmentId) {
        final Segment<K, V> out = Segment.<K, V>builder()
                .withDirectory(directory).withId(segmentId)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())
                .withMaxNumeberOfKeysInIndexPage(
                        conf.getMaxNumberOfKeysInSegmentIndexPage())
                .withValueTypeDescriptor(valueTypeDescriptor).build();
        return out;
    }

    public void forceCompactSegments() {
        /*
         * Defensive copy have to be done, because further splitting will affect
         * list size. In the future it will be slow.
         */
        final List<SegmentId> eligibleSegmentIds = segmentCache
                .getSegmentsAsStream().map(Pair::getValue)
                .collect(Collectors.toList());
        eligibleSegmentIds.forEach(segmentId -> {
            final Segment<K, V> segment = getSegment(segmentId);
            segment.forceCompact();
        });
    }

    /**
     * If number of keys reach threshold split segment into two.
     * 
     * @param sdf       required simple data file
     * @param segmentId required segment id
     * @return
     */
    private boolean optionallySplit(final Segment<K, V> sdf,
            final SegmentId segmentId) {
        if (sdf.getStats().getNumberOfKeys() > conf
                .getMaxNumberOfKeysInSegment()) {
            logger.debug("Splitting of '{}' started.", segmentId);
            final SegmentId newSegmentId = segmentCache.findNewSegmentId();
            //FIXME add SimpleDataFile.split to segment 
//            final K newPageKey = sdf.split(newSegmentId.getName());
//            segmentCache.insertSegment(newPageKey, newSegmentId);
            logger.debug("Splitting of segment '{}' to '{}' is done.",
                    segmentId, newSegmentId);
            return true;
        }
        return false;
    }

    @Override
    public V get(final K key) {
        Objects.requireNonNull(key, "Key cant be null");

        V out = cache.get(key);
        if (out == null) {
            // TODO record is not in memory try to look at disk
        }

        return out;
    }

    @Override
    public void delete(final K key) {
        Objects.requireNonNull(key, "Key cant be null");

        cache.put(Pair.of(key, valueTypeDescriptor.getTombstone()));
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

}
