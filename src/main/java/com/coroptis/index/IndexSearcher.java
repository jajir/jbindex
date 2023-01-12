package com.coroptis.index;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.sorteddatafile.DiffKeyReader;
import com.coroptis.index.sorteddatafile.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFileReader;
import com.coroptis.index.sorteddatafile.SortedDataFileStreamer;
import com.coroptis.index.type.ConvertorFromBytes;
import com.coroptis.index.type.OperationType;
import com.coroptis.index.type.TypeConvertors;
import com.coroptis.index.type.TypeDescriptorInteger;
import com.coroptis.index.type.TypeReader;

/**
 * Index is ordered list of unique keys with associated values.
 * 
 * @author jajir
 *
 * @param <K>
 * @param <V>
 */
public class IndexSearcher<K, V> {

    private final static Integer INVALID_BLOCK_ID = -2;

    private final List<Pair<K, Integer>> metaIndex = new ArrayList<>();
    private final Directory directory;
    private final ConvertorFromBytes<K> keyConvertorToBytes;
    private final TypeReader<V> valueReader;
    private final TypeDescriptorInteger integerTypeDescriptor = new TypeDescriptorInteger();
    private final Comparator<? super K> keyComparator;
    private final IndexDesc indexDesc;

    public IndexSearcher(final Directory directory, final Class<?> keyClass, final Class<?> valueClass) {
	this.directory = Objects.requireNonNull(directory, "directory must not be null");
	final TypeConvertors tc = TypeConvertors.getInstance();
	this.keyConvertorToBytes = tc.get(keyClass, OperationType.CONVERTOR_FROM_BYTES);
	this.valueReader = tc.get(valueClass, OperationType.READER);
	this.indexDesc = IndexDesc.load(directory);
	this.keyComparator = Objects.requireNonNull(tc.get(keyClass, OperationType.COMPARATOR),
		"keyComparator must not be null");
	loadMetaIndex();
    }

    public V get(final K key) {
	Objects.requireNonNull(key, "key must not be null");
	final Integer blockId = getBlock(key);
	if (blockId < 0) {
	    return null;
	}
	try (final SortedDataFileReader<K, V> mainIndexReader = getMainIndexReader()) {
	    mainIndexReader.skip(blockId);
	    final Optional<Pair<K, V>> oVal = mainIndexReader.stream(indexDesc.getWrittenKeyCount())
		    .limit(indexDesc.getBlockSize()).filter(pair -> keyComparator.compare(pair.getKey(), key) == 0)
		    .findFirst();
	    if (oVal.isPresent()) {
		return oVal.get().getValue();
	    }
	}
	return null;
    }

    public IndexStreamer<K, V> getStreamer() {
	final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(keyConvertorToBytes);
	return new IndexStreamer<>(directory, IndexWriter.INDEX_MAIN_DATA_FILE, diffKeyReader, valueReader,
		keyComparator, indexDesc.getWrittenKeyCount());
    }

    public IndexIterator<K, V> getIterator() {
	final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(keyConvertorToBytes);
	return new IndexIterator<>(directory.getFileReader(IndexWriter.INDEX_MAIN_DATA_FILE), diffKeyReader,
		valueReader);
    }

    private void loadMetaIndex() {
	try (final SortedDataFileStreamer<K, Integer> sir = new SortedDataFileStreamer<>(directory,
		IndexWriter.INDEX_META_FILE, keyConvertorToBytes, integerTypeDescriptor.getReader(), keyComparator)) {
	    sir.stream(indexDesc.getWrittenBlockCount()).forEach(pair -> metaIndex.add(pair));
	}
    }

    private Integer getBlock(final K key) {
	Pair<K, Integer> previousPair = null;
	for (final Pair<K, Integer> blockPair : metaIndex) {
	    if (keyComparator.compare(blockPair.getKey(), key) > 0) {
		/**
		 * key doesn't belongs to this block. So it belong to previous one.
		 */
		if (previousPair == null) {
		    return INVALID_BLOCK_ID; // no such block
		} else {
		    return previousPair.getValue();
		}
	    }
	    previousPair = blockPair;
	}

	return previousPair == null ? INVALID_BLOCK_ID : previousPair.getValue();
    }

    private SortedDataFileReader<K, V> getMainIndexReader() {
	return new SortedDataFileReader<>(directory, IndexWriter.INDEX_MAIN_DATA_FILE, keyConvertorToBytes, valueReader,
		keyComparator);
    }
}
