package com.coroptis.index.fastindex;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.type.TypeDescriptor;
import com.coroptis.index.type.TypeDescriptorInteger;

/**
 * Provide information about keys and particular index files.
 * 
 * @author honza
 *
 * @param <K>
 */
public class FastIndexFile<K> implements CloseableResource {

    private final static String FILE_NAME = "index.map";

    private TreeMap<K, Integer> list;
    private final SortedDataFile<K, Integer> sdf;
    private final Comparator<K> keyComparator;

    FastIndexFile(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor) {
        final TypeDescriptorInteger itd = new TypeDescriptorInteger();
        this.keyComparator = Objects
                .requireNonNull(keyTypeDescriptor.getComparator());
        this.sdf = new SortedDataFile<>(directory, FILE_NAME,
                itd.getTypeWriter(), itd.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
        this.list = new TreeMap<>(keyComparator);
        try (final PairIterator<K, Integer> reader = sdf.openIterator()) {
            while (reader.hasNext()) {
                final Pair<K, Integer> pair = reader.next();
                list.put(pair.getKey(), pair.getValue());
            }
        }
    }

    public Integer findSegmentId(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Pair<K, Integer> pair = localFindSegmentForKey(key);
        return pair == null ? null : pair.getValue();
    }

    public int insertKeyToSegment(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        final Pair<K, Integer> pair = localFindSegmentForKey(key);
        if (pair == null) {
            /*
             * Key is bigger that all key so it will at last segment. But key at
             * last segment is smaller than adding one. Because of that key have
             * to be upgraded.
             */
            return updateMaxKey(key);
        } else {
            return pair.getValue();
        }
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

    private int updateMaxKey(final K key) {
        if (list.size() == 0) {
            list.put(key, 0);
            return 0;
        } else {
            final Pair<K, Integer> max = Pair.of(list.lastEntry().getKey(),
                    list.lastEntry().getValue());
            list.remove(max.getKey());
            final Pair<K, Integer> newMax = Pair.of(key, max.getValue());
            list.put(newMax.getKey(), newMax.getValue());
            return newMax.getValue();
        }
    }

    public void insertSegment(final K key, final Integer pageId) {
        Objects.requireNonNull(key, "Key can't be null");
        list.put(key, pageId);
    }

    public Stream<Pair<K, Integer>> getPagesAsStream() {
        return list.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()));
    }

    public void flush() {
        try (final SortedDataFileWriter<K, Integer> writer = sdf.openWriter()) {
            list.forEach((k, v) -> writer.put(Pair.of(k, v)));
        }
    }

    @Override
    public void close() {
        flush();
    }

}
