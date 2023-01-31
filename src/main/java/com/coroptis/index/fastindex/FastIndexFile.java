package com.coroptis.index.fastindex;

import java.util.List;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.DataFileIterator;
import com.coroptis.index.Pair;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.SortedDataFile;
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

    private List<Pair<K, Integer>> map;

    FastIndexFile(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor) {
        final TypeDescriptorInteger itd = new TypeDescriptorInteger();
        final SortedDataFile<K, Integer> sdf = new SortedDataFile<>(directory,
                FILE_NAME, itd.getTypeWriter(), itd.getTypeReader(),
                keyTypeDescriptor.getComparator(),
                keyTypeDescriptor.getConvertorFromBytes(),
                keyTypeDescriptor.getConvertorToBytes());
        try (final DataFileIterator<K, Integer> reader = sdf.openIterator()) {
            while (reader.hasNext()) {
                final Pair<K, Integer> pair = reader.next();
                map.add(pair);
            }
        }
    }
    
    public Integer findFileId(final K key) {
        return null;
    }
    
    public void insertPage(final K key, final Integer pageId) {
        
    }
    
    public void save() {
        
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
