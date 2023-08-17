package com.coroptis.index.sst;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.basic.ValueMerger;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

public class SstIndexImpl<K,V> implements Index<K,V>, CloseableResource {

    private final long maxNumberOfKeysInCache;
    private final long maxNumeberOfKeysInSegmentCache;
    private final long maxNumeberOfKeysInSegment;
    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final UniqueCache<K,V> cache;

    public SstIndexImpl(final Directory directory,
            final ValueMerger<K, V> valueMerger,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor,
            final long maxNumberOfKeysInCache,
            final long maxNumeberOfKeysInSegmentCache,
            final long maxNumeberOfKeysInSegment) {
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.maxNumberOfKeysInCache = Objects
                .requireNonNull(maxNumberOfKeysInCache);
        this.maxNumeberOfKeysInSegmentCache = Objects
                .requireNonNull(maxNumeberOfKeysInSegmentCache);
        this.maxNumeberOfKeysInSegment = Objects
                .requireNonNull(maxNumeberOfKeysInSegment);

        this.cache = new UniqueCache<K,V>(this.keyTypeDescriptor.getComparator());
    }

    @Override
    public void put(final K key,final V value) {
        Objects.requireNonNull(key,"Key cant be null");
        Objects.requireNonNull(value,"Value cant be null");

        if ( valueTypeDescriptor.isTombstone(value)){
            throw new IllegalArgumentException(String.format("Can't insert thombstone value '%s' into index", value));
        }

        cache.add(Pair.of(key,value));
        
        // TODO add key value pair into WAL
    }

    @Override
    public V get(final K key) {
        Objects.requireNonNull(key,"Key cant be null");

        V out = cache.get(key);
        if(out == null){
        // TODO record is not in memory try to look at disk
        }

        return out;
    }

    @Override
    public void delete(final K key) {
        Objects.requireNonNull(key,"Key cant be null");
        
        cache.add(Pair.of(key, valueTypeDescriptor.getTombstone()));
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    
}
