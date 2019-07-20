package com.coroptis.index;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.simpleindex.Pair;
import com.coroptis.index.simpleindex.SimpleIndexReader;
import com.coroptis.index.type.ConvertorFromBytes;
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

    private final List<Pair<K, Integer>> metaIndex = new ArrayList<>();

    private final Directory directory;

    private final ConvertorFromBytes<K> keyConvertor;

    private final TypeReader<V> valueReader;

    private final TypeDescriptorInteger integerTypeDescriptor = new TypeDescriptorInteger();

    private final Comparator<? super K> keyComparator;

    private final IndexDesc indexDesc;

    public IndexSearcher(final Directory directory, final ConvertorFromBytes<K> convertorKey,
	    final Comparator<? super K> keyComparator, final TypeReader<V> valueReader) {
	this.directory = Objects.requireNonNull(directory, "directory must not be null");
	this.keyConvertor = Objects.requireNonNull(convertorKey, "key convertor must not be null");
	this.valueReader = Objects.requireNonNull(valueReader, "value convertor must not be null");
	this.indexDesc = IndexDesc.load(directory);
	this.keyComparator = Objects.requireNonNull(keyComparator,
		"keyComparator must not be null");
	loadMetaIndex();
    }

    public V get(final K key) {
	Objects.requireNonNull(key, "key must not be null");
	final Integer blockId = getBlock(key);
	if (blockId < 0) {
	    return null;
	}
	try (final SimpleIndexReader<K, V> mainIndexReader = getMainIndexReader()) {
	    mainIndexReader.skip(blockId);
	    final Optional<Pair<K, V>> oVal = mainIndexReader.stream(indexDesc.getWrittenKeyCount())
		    .limit(indexDesc.getBlockSize())
		    .filter(pair -> keyComparator.compare(pair.getKey(), key) == 0).findFirst();
	    if (oVal.isPresent()) {
		return oVal.get().getValue();
	    }
	}
	return null;
    }

    public IndexStreamer<K, V> getStreamer() {
	return new IndexStreamer<>(directory, IndexWriter.INDEX_MAIN_DATA_FILE, keyConvertor,
		valueReader, keyComparator, indexDesc.getWrittenKeyCount());
    }

    @Deprecated
    public Stream<Pair<K, V>> stream() {
	try (final SimpleIndexReader<K, V> mainIndexReader = getMainIndexReader()) {
	    return mainIndexReader.stream(indexDesc.getWrittenKeyCount());
	}
    }

    private void loadMetaIndex() {
	try (final SimpleIndexReader<K, Integer> sir = new SimpleIndexReader<>(
		directory.getFileReader(IndexWriter.INDEX_META_FILE), keyConvertor,
		integerTypeDescriptor.getReader(), keyComparator)) {
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
		    return -2; // no such block
		} else {
		    return previousPair.getValue();
		}
	    }
	    previousPair = blockPair;
	}
	return previousPair.getValue();
    }

    private SimpleIndexReader<K, V> getMainIndexReader() {
	return new SimpleIndexReader<>(directory.getFileReader(IndexWriter.INDEX_MAIN_DATA_FILE),
		keyConvertor, valueReader, keyComparator);
    }
}
