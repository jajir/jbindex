package com.coroptis.index.segment;

import java.util.List;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.sstfile.SstFile;

/**
 * Represents segment cache containing changes in segment.
 * 
 * In constructor are data loaded from file system.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDeltaCache<K, V> {

    private final SegmentFiles<K, V> segmentFiles;

    private final UniqueCache<K, V> cache;

    public SegmentDeltaCache(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.cache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyTypeDescriptor.getComparator())
                .withSstFile(segmentFiles.getCacheSstFile()).build();
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentDeltaFileName -> loadDataFromSegmentDeltaFile(segmentFiles,
                        segmentDeltaFileName));
    }

    private void loadDataFromSegmentDeltaFile(final SegmentFiles<K, V> segmentFiles,
            final String segmentDeltaFileName) {
        final SstFile<K, V> sstFile = segmentFiles
                .getCacheSstFile(segmentDeltaFileName);
        try (final PairIterator<K, V> iterator = sstFile.openIterator()) {
            while (iterator.hasNext()) {
                cache.put(iterator.next());
            }
        }
    }

    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        cache.put(pair);
    }

    public int size() {
        return cache.size();
    }

    public int sizeWithoutTombstones() {
        final TypeDescriptor<V> valueTypeDescriptor = segmentFiles
                .getValueTypeDescriptor();
        return (int) cache.getStream().filter(
                pair -> !valueTypeDescriptor.isTombstone(pair.getValue()))
                .count();
    }

    public void evictAll() {
        cache.clear();
    }

    public V get(final K key) {
        return cache.get(key);
    }

    public List<K> getSortedKeys() {
        return cache.getSortedKeys();
    }

    public List<Pair<K, V>> getAsSortedList() {
        return cache.getAsSortedList();
    }

    @Deprecated
    public PairIterator<K, V> getSortedIterator() {
        return cache.getSortedIterator();
    }

}
