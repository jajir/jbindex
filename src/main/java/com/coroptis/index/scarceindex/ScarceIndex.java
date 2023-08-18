package com.coroptis.index.scarceindex;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * Scarce index contain map that contain just subset of keys from SST. It's a map 'key,integer' records orderd by key value. It allows faster search for exact key in SST.
 * 
 * Scarce index is writen during process of creating main SST file. Later can't be changed. 
 */
public class ScarceIndex<K> {
    
    private final static TypeDescriptorInteger typeDescriptorInteger = new TypeDescriptorInteger();

    private final TypeDescriptor<K> keyTypeDescriptor;
 
    private final Directory directory;

    private final String fileName;

    private final SstFile<K,Integer> sstFile;

    private ScarceIndexCache<K> cache;

    public static <M> ScarceIndexBuilder<M> builder() {
        return new ScarceIndexBuilder<M>();
    }

    ScarceIndex(final Directory directory, final String fileName, final TypeDescriptor<K> keyTypeDescriptor){
        this.directory = Objects.requireNonNull(directory, "Directory object is null.");
        this.fileName = Objects.requireNonNull(fileName, "File name object is null.");
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor, "Key type descriptor object is null.");
        this.sstFile = new SstFile<>(directory, fileName,
                typeDescriptorInteger.getTypeWriter(),
                typeDescriptorInteger.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
        this.cache = new ScarceIndexCache<>(keyTypeDescriptor);
        loadCache();
    }

    void loadCache(){
        ScarceIndexCache<K> tmp =new ScarceIndexCache<>(keyTypeDescriptor);
        if(directory.isFileExists(fileName)){
        try (final PairIterator<K, Integer> reader = sstFile.openIterator()) {
            while (reader.hasNext()) {
                final Pair<K, Integer> pair = reader.next();
                tmp.put(pair);
            }
        }
        }
        tmp.sanityCheck();
        this.cache=tmp;
    }

    public Integer get(final K key){
        return cache.findSegmentId(key);
    }

    public ScarceIndexWriter<K> openWriter(){
        return new ScarceIndexWriter<>( this,sstFile.openWriter());
    }

}
