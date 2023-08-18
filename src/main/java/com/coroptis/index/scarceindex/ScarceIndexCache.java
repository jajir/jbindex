package com.coroptis.index.scarceindex;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.fastindex.ScarceIndexFileOld;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * Provide information about keys and particular index files. Each key
 * represents part of index equal or smaller to given key. Last key represents
 * higher key in index. When new value in index is entered it should be called
 * {@link #insertKeyToSegment(Object)}. This method update higher key value when
 * it's necessary.
 * 
 * @author honza
 *
 * @param <K>
 */
public class ScarceIndexCache<K> {
    
    private final Logger logger = LoggerFactory
            .getLogger(ScarceIndexCache.class);

    private final TreeMap<K, Integer> list;

    ScarceIndexCache(
            final TypeDescriptor<K> keyTypeDescriptor) {
        Objects
                .requireNonNull(keyTypeDescriptor.getComparator());
        this.list = new TreeMap<>(keyTypeDescriptor.getComparator());
    }

    void put(final Pair<K, Integer> pair){
        Objects.requireNonNull(pair,"Pair is null.");
        list.put(pair.getKey(), pair.getValue());
    }

    public void sanityCheck() {
        final HashMap<Integer, K> tmp = new HashMap<Integer, K>();
        final AtomicBoolean fail = new AtomicBoolean(false);
        list.forEach((key, segmentId) -> {
            final K oldKey = tmp.get(segmentId);
            if (oldKey == null) {
                tmp.put(segmentId, key);
            } else {
                logger.error(String.format(
                        "Segment id '%s' is used for segment with "
                                + "key '%s' and segment with key '%s'.",
                        segmentId, key, oldKey));
                fail.set(true);
            }
        });
        if (fail.get()) {
            throw new IllegalStateException(
                    "Unable to load scarce index, sanity check failed.");
        }
    }

    public Integer findSegmentId(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Pair<K, Integer> pair = localFindSegmentForKey(key);
        return pair == null ? null : pair.getValue();
    }

    public SegmentId findNewSegmentId() {
        return SegmentId.of((int) (getSegmentsAsStream().count()));
    }

    private Pair<K, Integer> localFindSegmentForKey(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Map.Entry<K, Integer> ceilingEntry = list.ceilingEntry(key);
        if (ceilingEntry == null) {
            return null;
        } else {
            return Pair.of(ceilingEntry.getKey(), ceilingEntry.getValue());
        }
    }

    public void clear(){
        list.clear();
    }

    public Stream<Pair<K, Integer>> getSegmentsAsStream() {
        return list.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()));
    }

}
