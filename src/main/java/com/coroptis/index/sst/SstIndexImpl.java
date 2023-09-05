package com.coroptis.index.sst;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static <M, N> SstIndexBuilder<M, N> builder() {
        return new SstIndexBuilder<>();
    }

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

    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair, "Pair cant be null");
        put(pair.getKey(), pair.getValue());
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

    public List<SegmentId> getSegmentIds() {
        return segmentCache.getSegmentsAsStream().map(pair -> pair.getValue())
                .collect(Collectors.toUnmodifiableList());
    }

    public Stream<Pair<K, V>> getSegmentStream(final SegmentId segmentId) {
        return getSegment(segmentId).getStream();
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
        List<SegmentId> segmentIds = support.getEligibleSegmentIds();
        segmentIds.stream().map(this::getSegment)
                .forEach(this::optionallySplit);
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
                .withMaxNumberOfKeysInIndexPage(
                        conf.getMaxNumberOfKeysInSegmentIndexPage())
                .withValueTypeDescriptor(valueTypeDescriptor).build();
        return out;
    }

    public void forceCompact() {
        compact();
    }

    /**
     * If number of keys reach threshold split segment into two.
     * 
     * @param segment required simple data file
     * @return
     */
    private boolean optionallySplit(final Segment<K, V> segment) {
        if (segment.getStats().getNumberOfKeys() > conf
                .getMaxNumberOfKeysInSegment()) {
            final SegmentId segmentId = segment.getId();
            logger.debug("Splitting of '{}' started.", segmentId);
            final SegmentId newSegmentId = segmentCache.findNewSegmentId();
            final Segment<K, V> splitted = segment.split(newSegmentId);
            segmentCache.insertSegment(splitted.getMaxKey(), newSegmentId);
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
            final SegmentId id = segmentCache.findSegmentId(key);
            final Segment<K, V> seg = getSegment(id);
            return seg.get(key);
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
        compact();
    }

}
