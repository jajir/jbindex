package com.coroptis.index.simpleindex;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFileWriter;
import com.coroptis.index.type.ConvertorToBytes;
import com.coroptis.index.type.TypeDescriptor;
import com.coroptis.index.type.TypeDescriptorInteger;
import com.coroptis.index.type.TypeWriter;

/**
 * Index contains following files:
 * <ul>
 * <li>main.dat - Contains ordered key value pairs. All data are here.</li>
 * <li>meta.dat - Holds index which is loaded into memory. Hold starting key of
 * block and pointer to same key into main.data index. It speed up key
 * search.</li>
 * <li>desc.txt - Hold index meta informations like block size.</li>
 * </ul>
 * <p>
 * Every key have to be smaller than following one.
 * </p>
 * 
 * @author jan
 *
 * @param <K>
 * @param <V>
 */
public class IndexWriter<K, V> implements CloseableResource {

    final static String INDEX_MAIN_DATA_FILE = "main.dat";

    final static String INDEX_META_FILE = "meta.dat";

    final static String INDEX_DESCRIPTION_FILE = "desc.dat";

    private final TypeDescriptorInteger integerTypeDescriptor = new TypeDescriptorInteger();

    private final SortedDataFileWriter<K, V> mainIndex;

    private final SortedDataFileWriter<K, Integer> metaIndex;

    private int previousPosition = 0;

    private final IndexDesc indexDesc;

    public IndexWriter(final Directory directory, final int blockSize,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        Objects.requireNonNull(keyTypeDescriptor, "Key type descriptor is null.");
        Objects.requireNonNull(valueTypeDescriptor, "Value type descriptor is null.");
        final ConvertorToBytes<K> keyConvertor = keyTypeDescriptor.getConvertorToBytes();
        final TypeWriter<V> valueWriter = valueTypeDescriptor.getTypeWriter();
        final Comparator<? super K> keyComparator = keyTypeDescriptor.getComparator();

        this.mainIndex = new SortedDataFileWriter<>(directory, INDEX_MAIN_DATA_FILE, keyConvertor,
                keyComparator, valueWriter);
        this.metaIndex = new SortedDataFileWriter<>(directory, INDEX_META_FILE, keyConvertor,
                keyComparator, integerTypeDescriptor.getTypeWriter());
        this.indexDesc = IndexDesc.create(directory, blockSize);
    }

    public void put(final K key, final V value) {
        Objects.requireNonNull(key);
        if (indexDesc.isBlockStart()) {
            previousPosition = mainIndex.put(new Pair<K, V>(key, value), true);
            metaIndex.put(new Pair<K, Integer>(key, previousPosition), false);
            indexDesc.incrementBlockCount();
        } else {
            previousPosition = mainIndex.put(new Pair<K, V>(key, value), false);
        }
        indexDesc.incrementWrittenKeyCount();
    }

    @Override
    public void close() {
        mainIndex.close();
        metaIndex.close();
        indexDesc.writeDescriptionFile();
    }

}
