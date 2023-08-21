package com.coroptis.index.segment;

import java.io.File;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;

public class Segment<K, V> implements CloseableResource {

    private final Directory directory;
    private final SegmentId id;
    private final long maxNumeberOfKeysInSegmentCache;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final Directory directory, final SegmentId id,
            final long maxNumeberOfKeysInSegmentCache,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.maxNumeberOfKeysInSegmentCache = maxNumeberOfKeysInSegmentCache;
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.cache = loadCache();
    }

    private UniqueCache<K, V> loadCache() {
        final SstFile<K, V> sstFile = getCacheSstFile();
        final UniqueCache<K, V> out = new UniqueCache<>(
                keyTypeDescriptor.getComparator());
        sstFile.openStreamer().stream().forEach(pair -> out.put(pair));
        return null;
    }

    private SstFile<K, V> getCacheSstFile() {
        return new SstFile<>(directory, id.getName() + File.separator + "cache",
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
    }

    UniqueCache<K, V> getCache() {
        return cache;
    }

    void flushCache() {
        if (cache.size() > maxNumeberOfKeysInSegmentCache) {
            forceCompact();
        } else {
            try (final SstFileWriter<K, V> writer = getCacheSstFile()
                    .openWriter()) {
                cache.getStream().forEach(writer::put);
            }
        }
    }

    void optionallyCompact() {
        if (cache.size() > maxNumeberOfKeysInSegmentCache) {
            forceCompact();
        }
    }

    void forceCompact() {
        
    }

    public Object get(Object key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'get'");
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

}
