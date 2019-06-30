package com.coroptis.index;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.simpleindex.CloseableResource;
import com.coroptis.index.simpleindex.Pair;
import com.coroptis.index.simpleindex.SimpleIndexWriter;
import com.coroptis.index.storage.Directory;
import com.coroptis.index.type.IntegerTypeDescriptor;
import com.coroptis.index.type.TypeArrayWriter;
import com.coroptis.index.type.TypeRawArrayWriter;

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

    private final IntegerTypeDescriptor integerTypeDescriptor = new IntegerTypeDescriptor();

    private final SimpleIndexWriter<K, V> mainIndex;

    private final SimpleIndexWriter<K, Integer> metaIndex;

    private int previousPosition = 0;

    private final IndexDesc indexDesc;

    public IndexWriter(final Directory directory, final int blockSize,
	    final TypeRawArrayWriter<K> keyTypeRawArrayWriter, final Comparator<? super K> keyComparator,
	    final TypeArrayWriter<V> valueTypeArrayWriter) {
	this.mainIndex = new SimpleIndexWriter<>(directory.getFileWriter(INDEX_MAIN_DATA_FILE), keyTypeRawArrayWriter,
		keyComparator, valueTypeArrayWriter);
	this.metaIndex = new SimpleIndexWriter<>(directory.getFileWriter(INDEX_META_FILE), keyTypeRawArrayWriter,
		keyComparator, integerTypeDescriptor.getArrayWriter());
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
