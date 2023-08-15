package com.coroptis.index.segment;

import java.io.File;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sst.Index;
import com.coroptis.index.sstfile.SstFile;

public class Segment<K, V> implements CloseableResource, Index {

    private final Directory directory;
    private final SegmentId id;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K, V> cache;

    public static <M, N> SegmentBuilder<M, N> builder() {
        return new SegmentBuilder<>();
    }

    public Segment(final Directory directory, final SegmentId id,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.cache = loadCache();
    }

    private UniqueCache<K, V> loadCache() {
        final SstFile<K, V> sstFile = new SstFile<>(directory,
                id.getName() + File.separator + "cache",
                valueTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
        final UniqueCache<K, V> out = new UniqueCache<>(
                keyTypeDescriptor.getComparator());
        sstFile.openStreamer().stream().forEach(pair -> out.add(pair));
        return null;
    }

    @Override
    public void put(Object key, Object value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'put'");
    }

    @Override
    public Object get(Object key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'get'");
    }

    @Override
    public void delete(Object key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(
                "Unimplemented method 'delete'");
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

}
