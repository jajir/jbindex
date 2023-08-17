package com.coroptis.index.scarceindex;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

/**
 * Scarce index contain map that contain just subset of keys from SST. It's a map 'key,integer' records orderd by key value. It allows faster search for exact key in SST.
 * 
 * Scarce index is writen during process of creating main SST file. Later can't be changed. 
 */
public class ScarceIndex<K> {
    
    private final TypeDescriptor<K> keyTypeDescriptor;
 
    private final Directory directory;

    private final String fileName;

    public static <M> ScarceIndexBuilder<M> builder() {
        return new ScarceIndexBuilder<M>();
    }

    ScarceIndex(final Directory directory, final String fileName, final TypeDescriptor<K> keyTypeDescriptor){
        this.directory = Objects.requireNonNull(directory, "Directory object is null.");
        this.fileName = Objects.requireNonNull(fileName, "File name object is null.");
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor, "Key type descriptor object is null.");
    }

    ScarceIndexWriter<K> openWriter(){
        return null;
    }

}
